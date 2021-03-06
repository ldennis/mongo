/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "../connection_manager.h"
#include "../core/configuration.h"
#include "../timestamp_manager.h"
#include "../util/api_const.h"
#include "workload_tracking.h"
#include "random_generator.h"
#include "thread_context.h"

namespace test_harness {
/* transaction_context class implementation */
transaction_context::transaction_context(
  configuration *config, timestamp_manager *timestamp_manager)
    : _timestamp_manager(timestamp_manager)
{
    /* Use optional here as our populate threads don't define this configuration. */
    configuration *transaction_config = config->get_optional_subconfig(OPS_PER_TRANSACTION);
    if (transaction_config != nullptr) {
        _min_op_count = transaction_config->get_optional_int(MIN, 1);
        _max_op_count = transaction_config->get_optional_int(MAX, 1);
        delete transaction_config;
    }
}

bool
transaction_context::active() const
{
    return (_in_txn);
}

void
transaction_context::add_op()
{
    _op_count++;
}

void
transaction_context::begin(WT_SESSION *session, const std::string &config)
{
    testutil_assert(!_in_txn);
    testutil_check(session->begin_transaction(session, config.empty() ? nullptr : config.c_str()));
    /* This randomizes the number of operations to be executed in one transaction. */
    _target_op_count =
      random_generator::instance().generate_integer<int64_t>(_min_op_count, _max_op_count);
    _op_count = 0;
    _in_txn = true;
}

void
transaction_context::try_begin(WT_SESSION *session, const std::string &config)
{
    if (!_in_txn)
        begin(session, config);
}

void
transaction_context::commit(WT_SESSION *session, const std::string &config)
{
    testutil_assert(_in_txn);
    testutil_check(session->commit_transaction(session, config.empty() ? nullptr : config.c_str()));
    _op_count = 0;
    _in_txn = false;
}

void
transaction_context::try_commit(WT_SESSION *session, const std::string &config)
{
    if (can_commit_rollback())
        commit(session, config);
}

void
transaction_context::rollback(WT_SESSION *session, const std::string &config)
{
    testutil_assert(_in_txn);
    testutil_check(
      session->rollback_transaction(session, config.empty() ? nullptr : config.c_str()));
    _op_count = 0;
    _in_txn = false;
}

void
transaction_context::try_rollback(WT_SESSION *session, const std::string &config)
{
    if (can_commit_rollback())
        rollback(session, config);
}

void
transaction_context::set_commit_timestamp(WT_SESSION *session, wt_timestamp_t ts)
{
    /* We don't want to set zero timestamps on transactions if we're not using timestamps. */
    if (!_timestamp_manager->enabled())
        return;
    std::string config = std::string(COMMIT_TS) + "=" + timestamp_manager::decimal_to_hex(ts);
    testutil_check(session->timestamp_transaction(session, config.c_str()));
}

bool
transaction_context::can_commit_rollback()
{
    return (_in_txn && _op_count >= _target_op_count);
}

/* thread_context class implementation */
thread_context::thread_context(uint64_t id, thread_type type, configuration *config,
  timestamp_manager *timestamp_manager, workload_tracking *tracking, database &dbase)
    : id(id), type(type), db(dbase), tsm(timestamp_manager), tracking(tracking),
      transaction(transaction_context(config, timestamp_manager)),
      /* These won't exist for certain threads which is why we use optional here. */
      collection_count(config->get_optional_int(COLLECTION_COUNT, 1)),
      key_count(config->get_optional_int(KEY_COUNT_PER_COLLECTION, 1)),
      key_size(config->get_optional_int(KEY_SIZE, 1)),
      value_size(config->get_optional_int(VALUE_SIZE, 1)),
      thread_count(config->get_int(THREAD_COUNT))
{
    session = connection_manager::instance().create_session();
    _throttle = throttle(config);

    if (tracking->enabled())
        op_track_cursor = session.open_scoped_cursor(tracking->get_operation_table_name().c_str());

    testutil_assert(key_size > 0 && value_size > 0);
}

void
thread_context::finish()
{
    _running = false;
}

std::string
thread_context::key_to_string(uint64_t key_id)
{
    std::string str, value_str = std::to_string(key_id);
    testutil_assert(key_size >= value_str.size());
    uint64_t diff = key_size - value_str.size();
    std::string s(diff, '0');
    str = s.append(value_str);
    return (str);
}

bool
thread_context::update(scoped_cursor &cursor, uint64_t collection_id, const std::string &key)
{
    WT_DECL_RET;
    std::string value;
    wt_timestamp_t ts = tsm->get_next_ts();
    testutil_assert(tracking != nullptr);
    testutil_assert(cursor.get() != nullptr);

    transaction.set_commit_timestamp(session.get(), ts);
    value = random_generator::instance().generate_string(value_size);
    cursor->set_key(cursor.get(), key.c_str());
    cursor->set_value(cursor.get(), value.c_str());
    ret = cursor->update(cursor.get());
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.rollback(session.get(), "");
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to update a key");
    }
    ret = tracking->save_operation(
      tracking_operation::INSERT, collection_id, key.c_str(), value.c_str(), ts, op_track_cursor);
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.rollback(session.get(), "");
            return (false);
        } else
            testutil_die(
              ret, "unhandled error while trying to save an update to the tracking table");
    }
    transaction.add_op();
    return (true);
}

bool
thread_context::insert(scoped_cursor &cursor, uint64_t collection_id, uint64_t key_id)
{
    WT_DECL_RET;
    std::string key, value;
    testutil_assert(tracking != nullptr);
    testutil_assert(cursor.get() != nullptr);

    /*
     * Get a timestamp to apply to the update. We still do this even if timestamps aren't enabled as
     * it will return a value for the tracking table.
     */
    wt_timestamp_t ts = tsm->get_next_ts();
    transaction.set_commit_timestamp(session.get(), ts);

    key = key_to_string(key_id);
    value = random_generator::instance().generate_string(value_size);

    cursor->set_key(cursor.get(), key.c_str());
    cursor->set_value(cursor.get(), value.c_str());
    ret = cursor->insert(cursor.get());
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.rollback(session.get(), "");
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to insert a key");
    }
    ret = tracking->save_operation(
      tracking_operation::INSERT, collection_id, key.c_str(), value.c_str(), ts, op_track_cursor);
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.rollback(session.get(), "");
            return (false);
        } else
            testutil_die(
              ret, "unhandled error while trying to save an insert to the tracking table");
    }
    transaction.add_op();
    return (true);
}

void
thread_context::sleep()
{
    _throttle.sleep();
}

bool
thread_context::running() const
{
    return (_running);
}
} // namespace test_harness
