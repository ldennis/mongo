/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/util/fail_point.h"

#include <memory>

#include "mongo/bson/util/bson_extract.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace {

/**
 * Type representing the per-thread PRNG used by fail-points.
 */
class FailPointPRNG {
public:
    FailPointPRNG() : _prng(std::unique_ptr<SecureRandom>(SecureRandom::create())->nextInt64()) {}

    void resetSeed(int32_t seed) {
        _prng = PseudoRandom(seed);
    }

    int32_t nextPositiveInt32() {
        return _prng.nextInt32() & ~(1 << 31);
    }

    static FailPointPRNG* current() {
        if (!_failPointPrng)
            _failPointPrng = std::make_unique<FailPointPRNG>();
        return _failPointPrng.get();
    }

private:
    PseudoRandom _prng;
    static thread_local std::unique_ptr<FailPointPRNG> _failPointPrng;
};

thread_local std::unique_ptr<FailPointPRNG> FailPointPRNG::_failPointPrng;

}  // namespace

std::unordered_set<std::string> FailPoint::_activeSignals;
stdx::mutex FailPoint::_syncMutex;
stdx::condition_variable FailPoint::_condVar;

void FailPoint::setThreadPRNGSeed(int32_t seed) {
    FailPointPRNG::current()->resetSeed(seed);
}

FailPoint::FailPoint() = default;

void FailPoint::shouldFailCloseBlock() {
    _fpInfo.subtractAndFetch(1);
}

bool FailPoint::isSynced() const {
    if (_syncConfig.waitFor.size() == 0)
        return true;
    for (auto w : _syncConfig.waitFor) {
        if (_activeSignals.find(w) == _activeSignals.end()) {
            return false;
        }
    }
    return true;
}

bool FailPoint::syncEnabled() const {
    return _syncConfig.enabled;
}

void FailPoint::sync() const {
    if (!syncEnabled()) {
        return;
    }
    stdx::unique_lock<stdx::mutex> lk(_syncMutex);

    for (auto& s : _syncConfig.signals) {
        _activeSignals.insert(s);
    }
    _condVar.notify_all();
    auto timeout = Seconds(60);
    while (!isSynced()) {
        _condVar.wait_for(lk, timeout.toSystemDuration());
    }
}

void FailPoint::setMode(Mode mode,
                        ValType val,
                        const BSONObj& extra,
                        const SyncConfig& syncConfig) {
    /**
     * Outline:
     *
     * 1. Deactivates fail point to enter write-only mode
     * 2. Waits for all current readers of the fail point to finish
     * 3. Sets the new mode.
     */

    stdx::lock_guard<stdx::mutex> scoped(_modMutex);

    // Step 1
    disableFailPoint();

    // Step 2
    while (_fpInfo.load() != 0) {
        sleepmillis(50);
    }

    _mode = mode;
    _timesOrPeriod.store(val);

    _data = extra.copy();

    if (_mode != off) {
        enableFailPoint();
    }

    _syncConfig = syncConfig;
}

const BSONObj& FailPoint::getData() const {
    return _data;
}

void FailPoint::enableFailPoint() {
    _fpInfo.fetchAndBitOr(ACTIVE_BIT);
}

void FailPoint::disableFailPoint() {
    _fpInfo.fetchAndBitAnd(~ACTIVE_BIT);
}

FailPoint::RetCode FailPoint::slowShouldFailOpenBlock(
    std::function<bool(const BSONObj&)> cb) noexcept {
    ValType localFpInfo = _fpInfo.addAndFetch(1);

    if ((localFpInfo & ACTIVE_BIT) == 0) {
        return slowOff;
    }

    if (cb && !cb(getData())) {
        return userIgnored;
    }

    switch (_mode) {
        case alwaysOn:
            return slowOn;
        case random: {
            const int maxActivationValue = _timesOrPeriod.load();
            if (FailPointPRNG::current()->nextPositiveInt32() < maxActivationValue)
                return slowOn;

            return slowOff;
        }
        case nTimes: {
            if (_timesOrPeriod.subtractAndFetch(1) <= 0)
                disableFailPoint();

            return slowOn;
        }
        case skip: {
            // Ensure that once the skip counter reaches within some delta from 0 we don't continue
            // decrementing it unboundedly because at some point it will roll over and become
            // positive again
            if (_timesOrPeriod.load() <= 0 || _timesOrPeriod.subtractAndFetch(1) < 0)
                return slowOn;

            return slowOff;
        }
        default:
            error() << "FailPoint Mode not supported: " << static_cast<int>(_mode);
            fassertFailed(16444);
    }
}

StatusWith<std::tuple<FailPoint::Mode, FailPoint::ValType, BSONObj, FailPoint::SyncConfig>>
FailPoint::parseBSON(const BSONObj& obj) {
    Mode mode = FailPoint::alwaysOn;
    ValType val = 0;
    const BSONElement modeElem(obj["mode"]);
    if (modeElem.eoo()) {
        return {ErrorCodes::IllegalOperation, "When setting a failpoint, you must supply a 'mode'"};
    } else if (modeElem.type() == String) {
        const std::string modeStr(modeElem.valuestr());

        if (modeStr == "off") {
            mode = FailPoint::off;
        } else if (modeStr == "alwaysOn") {
            mode = FailPoint::alwaysOn;
        } else {
            return {ErrorCodes::BadValue, str::stream() << "unknown mode: " << modeStr};
        }
    } else if (modeElem.type() == Object) {
        const BSONObj modeObj(modeElem.Obj());

        if (modeObj.hasField("times")) {
            mode = FailPoint::nTimes;

            long long longVal;
            auto status = bsonExtractIntegerField(modeObj, "times", &longVal);
            if (!status.isOK()) {
                return status;
            }

            if (longVal < 0) {
                return {ErrorCodes::BadValue, "'times' option to 'mode' must be positive"};
            }

            if (longVal > std::numeric_limits<int>::max()) {
                return {ErrorCodes::BadValue, "'times' option to 'mode' is too large"};
            }
            val = static_cast<int>(longVal);
        } else if (modeObj.hasField("skip")) {
            mode = FailPoint::skip;

            long long longVal;
            auto status = bsonExtractIntegerField(modeObj, "skip", &longVal);
            if (!status.isOK()) {
                return status;
            }

            if (longVal < 0) {
                return {ErrorCodes::BadValue, "'skip' option to 'mode' must be positive"};
            }

            if (longVal > std::numeric_limits<int>::max()) {
                return {ErrorCodes::BadValue, "'skip' option to 'mode' is too large"};
            }
            val = static_cast<int>(longVal);
        } else if (modeObj.hasField("activationProbability")) {
            mode = FailPoint::random;

            if (!modeObj["activationProbability"].isNumber()) {
                return {ErrorCodes::TypeMismatch,
                        "the 'activationProbability' option to 'mode' must be a double between 0 "
                        "and 1"};
            }

            const double activationProbability = modeObj["activationProbability"].numberDouble();
            if (activationProbability < 0 || activationProbability > 1) {
                return {ErrorCodes::BadValue,
                        str::stream() << "activationProbability must be between 0.0 and 1.0; found "
                                      << activationProbability};
            }
            val = static_cast<int32_t>(std::numeric_limits<int32_t>::max() * activationProbability);
        } else {
            return {
                ErrorCodes::BadValue,
                "'mode' must be one of 'off', 'alwaysOn', 'times', and 'activationProbability'"};
        }
    } else {
        return {ErrorCodes::TypeMismatch, "'mode' must be a string or JSON object"};
    }

    BSONObj data;
    if (obj.hasField("data")) {
        if (!obj["data"].isABSONObj()) {
            return {ErrorCodes::TypeMismatch, "the 'data' option must be a JSON object"};
        }
        data = obj["data"].Obj().getOwned();
    }

    SyncConfig syncConfig;
    const BSONElement syncElem(obj["sync"]);
    if (!syncElem.eoo()) {
        if (syncElem.type() != Object) {
            return {ErrorCodes::TypeMismatch, "'sync' must be a JSON object"};
        }
        const BSONObj syncObj(syncElem.Obj());
        if (syncObj.hasField("signals")) {
            const BSONElement signals(syncObj["signals"]);
            if (!signals.ok() || signals.type() != Array) {
                return {ErrorCodes::TypeMismatch, "'sync.signals' must be an array of strings"};
            }
            auto it = BSONObjIterator(signals.Obj());
            while (it.more()) {
                auto e = it.next();
                if (e.type() != String) {
                    return {ErrorCodes::TypeMismatch, "'sync.signals' must be an array of strings"};
                }
                syncConfig.signals.insert(e.String());
            }
        }
        if (syncObj.hasField("waitFor")) {
            const BSONElement waitFor(syncObj["waitFor"]);
            if (!waitFor.ok() || waitFor.type() != Array) {
                return {ErrorCodes::TypeMismatch, "'sync.waitFor' must be an array of strings"};
            }
            auto it = BSONObjIterator(waitFor.Obj());
            while (it.more()) {
                auto e = it.next();
                if (e.type() != String) {
                    return {ErrorCodes::TypeMismatch, "'sync.waitFor' must be an array of strings"};
                }
                syncConfig.waitFor.insert(e.String());
            }
        }
        syncConfig.enabled = true;
    }

    return std::make_tuple(mode, val, data, syncConfig);
}

BSONObj FailPoint::toBSON() const {
    BSONObjBuilder builder;

    stdx::lock_guard<stdx::mutex> scoped(_modMutex);
    builder.append("mode", _mode);
    builder.append("data", _data);

    return builder.obj();
}
}
