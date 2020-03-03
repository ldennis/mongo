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

#pragma once

#include <cstddef>
#include <vector>

#include <mongo/bson/timestamp.h>
#include <mongo/db/catalog/collection.h>
#include <mongo/db/namespace_string.h>
#include <mongo/db/operation_context.h>
#include <mongo/db/repl/oplog.h>
#include <mongo/db/service_context.h>
#include <mongo/stdx/mutex.h>

namespace mongo {
namespace repl {

/**
 * This structure contains per-service-context state related to the oplog.
 */
class LocalOplogInfo {
public:
    static LocalOplogInfo* get(ServiceContext& service);
    static LocalOplogInfo* get(ServiceContext* service);
    static LocalOplogInfo* get(OperationContext* opCtx);

    LocalOplogInfo(const LocalOplogInfo&) = delete;
    LocalOplogInfo& operator=(const LocalOplogInfo&) = delete;
    LocalOplogInfo() = default;

    /**
     * Returns namespace of the local oplog collection.
     */
    const NamespaceString& getOplogCollectionName() const;

    /**
     * Detects the current replication mode and sets the "_oplogName" accordingly.
     */
    void setOplogCollectionName(ServiceContext* service);

    Collection* getCollection() const;
    void setCollection(Collection* oplog);
    void resetCollection();

    /**
     * Sets the global Timestamp to be 'newTime'.
     */
    void setNewTimestamp(ServiceContext* opCtx, const Timestamp& newTime);

    /**
     * Allocates optimes for new entries in the oplog. Returns the new optimes in a vector along
     * with their terms.
     */
    std::vector<OplogSlot> getNextOpTimes(OperationContext* opCtx, std::size_t count);

private:
    // Name of the oplog collection.
    NamespaceString _oplogName;

    // The "oplog" pointer is always valid (or null) because an operation must take the global
    // exclusive lock to set the pointer to null when the Collection instance is destroyed. See
    // "oplogCheckCloseDatabase".
    Collection* _oplog = nullptr;

    // Synchronizes the section where a new Timestamp is generated and when it is registered in the
    // storage engine.
    mutable Mutex _newOpMutex = MONGO_MAKE_LATCH("LocaloplogInfo::_newOpMutex");
};

enum OplogAccessMode {
    OPLOG_READ = 0,
    OPLOG_WRITE = 1,
};

class AutoGetOplog {
    AutoGetOplog(const AutoGetOplog&) = delete;
    AutoGetOplog& operator=(const AutoGetOplog&) = delete;

public:
    explicit AutoGetOplog(OperationContext* opCtx,
                          OplogAccessMode mode,
                          Date_t deadline = Date_t::max())
        : _shouldNotConflictWithSecondaryBatchApplicationBlock(opCtx->lockState()),
          _globalLock(opCtx,
                      mode == OPLOG_READ ? MODE_IS : MODE_IX,
                      deadline,
                      Lock::InterruptBehavior::kThrow),
          _oplogInfo(LocalOplogInfo::get(opCtx)),
          _oplog(_oplogInfo->getCollection()) {
        uassert(ErrorCodes::NamespaceNotFound, "oplog collection does not exist", _oplog);
        // Obtain Collection exclusive intent write lock for non-document-locking storage engines.
        if (!opCtx->getServiceContext()->getStorageEngine()->supportsDocLocking()) {
            auto lockMode = (mode == OPLOG_WRITE) ? MODE_IX : MODE_IS;
            _dbWriteLock.emplace(opCtx, NamespaceString::kLocalDb, lockMode, deadline);
            _collWriteLock.emplace(
                opCtx, LocalOplogInfo::get(opCtx)->getOplogCollectionName(), lockMode, deadline);
        }
    }

    LocalOplogInfo* getOplogInfo() const {
        return _oplogInfo;
    }

    Collection* getCollection() const {
        return _oplog;
    }

private:
    ShouldNotConflictWithSecondaryBatchApplicationBlock
        _shouldNotConflictWithSecondaryBatchApplicationBlock;
    Lock::GlobalLock _globalLock;
    LocalOplogInfo* _oplogInfo;
    Collection* _oplog;
    boost::optional<Lock::DBLock> _dbWriteLock;
    boost::optional<Lock::CollectionLock> _collWriteLock;
};

}  // namespace repl
}  // namespace mongo
