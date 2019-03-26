/*
 * This test makes sure 'find' and 'getMore' commands fail correctly during rollback.
 */
(function() {
    "use strict";

    load("jstests/replsets/libs/rollback_test.js");

    const dbName = "test";
    const collName = "coll";

    let setFailPoint = (node, failpoint) => {
        jsTestLog("Setting fail point " + failpoint);
        assert.commandWorked(node.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));
    };

    let clearFailPoint = (node, failpoint) => {
        jsTestLog("Clearing fail point " + failpoint);
        assert.commandWorked(node.adminCommand({configureFailPoint: failpoint, mode: "off"}));
    };

    // Set up Rollback Test.
    let rollbackTest = new RollbackTest();

    // Insert a document.
    assert.commandWorked(rollbackTest.getPrimary().getDB(dbName)[collName].insert(
        {_id: 1, x: "document_of_interest"}));

    let rollbackNode = rollbackTest.transitionToRollbackOperations();

    setFailPoint(rollbackNode, "rollbackHangAfterTransitionToRollback");

    setFailPoint(rollbackNode, "GetMoreHangBeforeReadLock");

    const joinGetMoreThread = startParallelShell(() => {
        db.getMongo().setSlaveOk();
        const cursorID =
            assert.commandWorked(db.runCommand({"find": "coll", batchSize: 0})).cursor.id;
        let res = assert.throws(function() {
            db.runCommand({"getMore": cursorID, collection: "coll"});
        }, [], "network error");
        // Make sure connection get closed during rollback.
        assert.includes(res.toString(), "network error while attempting to run command");
    }, rollbackNode.port);

    const cursorIdToBeReadDuringRollback =
        assert
            .commandWorked(rollbackNode.getDB(dbName).runCommand({"find": collName, batchSize: 0}))
            .cursor.id;

    // Wait for 'getMore' to hang.
    checkLog.contains(rollbackNode, "GetMoreHangBeforeReadLock fail point enabled.");

    // Start rollback.
    rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
    rollbackTest.transitionToSyncSourceOperationsDuringRollback();

    jsTestLog("Reconnecting to " + rollbackNode.host + " after rollback");
    reconnect(rollbackNode.getDB(dbName));

    // Wait for rollback to hang.
    checkLog.contains(rollbackNode, "rollbackHangAfterTransitionToRollback fail point enabled.");

    clearFailPoint(rollbackNode, "GetMoreHangBeforeReadLock");

    jsTestLog("Wait for 'getMore' thread to join");
    joinGetMoreThread();

    jsTestLog("Reading during rollback");
    assert.commandFailedWithCode(rollbackNode.getDB(dbName).runCommand({"find": collName}),
                                 ErrorCodes.NotMasterOrSecondary);
    assert.commandFailedWithCode(
        rollbackNode.getDB(dbName).runCommand(
            {"getMore": cursorIdToBeReadDuringRollback, collection: collName}),
        ErrorCodes.NotMasterOrSecondary);

    setFailPoint(rollbackNode, "skipCheckingForNotMasterInCommandDispatch");
    jsTestLog("Reading during rollback (skip checks in service_entry_point_common.cpp)");
    assert.commandFailedWithCode(rollbackNode.getDB(dbName).runCommand({"find": collName}),
                                 ErrorCodes.NotMasterOrSecondary);
    assert.commandFailedWithCode(
        rollbackNode.getDB(dbName).runCommand(
            {"getMore": cursorIdToBeReadDuringRollback, collection: collName}),
        ErrorCodes.NotMasterOrSecondary);

    clearFailPoint(rollbackNode, "rollbackHangAfterTransitionToRollback");

    rollbackTest.transitionToSteadyStateOperations();

    // Check the replica set.
    rollbackTest.stop();
}());
