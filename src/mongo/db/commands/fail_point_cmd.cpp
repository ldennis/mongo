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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include <vector>

#include "mongo/base/init.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"

namespace mongo {

/**
 * Command for modifying installed fail points.
 *
 * Format
 * {
 *    configureFailPoint: <string>, // name of the fail point.
 *        If string value 'now' is passed in together with a 'sync' field, runs
 *        MONGO_FAIL_POINT_SYNC inline with the sync configuration passed.
 *
 *    mode: <string|Object>, // the new mode to set. Can have one of the
 *        following format:
 *
 *        1. 'off' - disable fail point.
 *        2. 'alwaysOn' - fail point is always active.
 *        3. { activationProbability: <n> } - n should be a double between 0 and 1,
 *           representing the probability that the fail point will fire.  0 means never,
 *           1 means (nearly) always.
 *        4. { times: <n> } - n should be positive and within the range of a 32 bit
 *            signed integer and this is the number of passes on the fail point will
 *            remain activated.
 *
 *    data: <Object> // optional arbitrary object to store.
 *    sync: <Object> // optional object that stores parameters used for failpoint synchronization.
 *                   // Has the following fields:
 *        signals - An array of strings representing names of signals to emit once a failpoint is
 *        triggered.
 *        waitFor - An array of strings representing names of signals to wait for before a failpoint
 *        can be unblocked.
 *        timeout - The number of seconds to wait for signals from the waitFor array before timing
 *        out.
 *        clearSignal - A boolean field representing whether to deactivate a signal once we have
 *        successfully waited for it.
 *
 *     Example:
 *        sync: {
 *          signals: [<named_signal1>, <named_signal2>],
 *          waitFor: [<named_signal1>, <named_signal2>],
 *          timeout: <seconds>,
 *          clearSignal: <true/false>
 *        }
 */
class FaultInjectCmd : public BasicCommand {
public:
    FaultInjectCmd() : BasicCommand("configureFailPoint") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    bool requiresAuth() const override {
        return false;
    }

    // No auth needed because it only works when enabled via command line.
    void addRequiredPrivileges(const std::string& dbname,
                               const BSONObj& cmdObj,
                               std::vector<Privilege>* out) const override {}

    std::string help() const override {
        return "modifies the settings of a fail point";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        const std::string failPointName(cmdObj.firstElement().str());
        if (failPointName == "now" && cmdObj.hasField("sync")) {
            // Perform synchronization inline when failpointName is 'now'.
            syncNow(opCtx, cmdObj);
        } else {
            setGlobalFailPoint(failPointName, cmdObj);
        }
        return true;
    }
};
MONGO_REGISTER_TEST_COMMAND(FaultInjectCmd);
}  // namespace mongo
