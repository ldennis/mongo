/*
 * This test makes sure find and get commands fail correctly during rollback
 */
(function() {
    "use strict";

    load("jstests/replsets/libs/rollback_test.js");

    TestData.rollbackShutdowns = true;
    TestData.allowUncleanShutdowns = true;
    let pass = 1;
    const dbName = "test";
    const sourceCollName = "coll";

    let doc1 = {_id: 1, x: "document_of_interest"};
    let doc2 = {_id: 2, x: "document_of_interest"};

    let CommonOps = (node) => {
        // Insert a document that will exist on all nodes.
        assert.commandWorked(node.getDB(dbName)[sourceCollName].insert(doc1));
        assert.commandWorked(node.getDB(dbName)[sourceCollName].insert(doc2));
    };

    let RollbackOps = (node) => {
        // Delete the document on rollback node so it will be refetched from sync source.
        assert.commandWorked(node.getDB(dbName)[sourceCollName].remove({_id: 1}));
    };

    let SetFailPoint = (node, failpoint) => {
        jsTestLog("Setting fail point " + failpoint);
        assert.commandWorked(node.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));
    };

    let ClearFailPoint = (node, failpoint) => {
        jsTestLog("Clearing fail point " + failpoint);
        assert.commandWorked(node.adminCommand({configureFailPoint: failpoint, mode: "off"}));
    };

    let findShouldFailDuringRollback = (node) => {
        try {
            assert.commandFailedWithCode(node.getDB(dbName).runCommand({"find": sourceCollName}),
                                         ErrorCodes.NotMasterOrSecondary);
        } catch (e) {
            print("FAILED: " + e);
            pass = 0;
        }
    };

    let getMoreShouldFailDuringRollback = (node, cursorID) => {
        try {
            assert.commandFailedWithCode(
                node.getDB(dbName).runCommand({"getMore": cursorID, collection: sourceCollName}),
                ErrorCodes.NotMasterOrSecondary);
        } catch (e) {
            print("FAILED: " + e);
            pass = 0;
        }
    };

    // Set up Rollback Test.
    let rollbackTest = new RollbackTest();
    CommonOps(rollbackTest.getPrimary());

    jsTestLog("transitionToSteadyStateOperations");
    let rollbackNode = rollbackTest.transitionToRollbackOperations();

    SetFailPoint(rollbackNode, "rollbackHangAfterTransitionToRollback");

    SetFailPoint(rollbackNode, "GetMoreHangBeforeReadlock");

    jsTestLog("Start blocking getMore cmd before rollback");
    const joinGetMoreThread = startParallelShell(() => {
        db.getMongo().setSlaveOk();
        const cursorID =
            assert.commandWorked(db.runCommand({"find": "coll", batchSize: 0})).cursor.id;
        let res = assert.throws(function() {
            db.runCommand({"getMore": cursorID, collection: "coll"});
        }, [], "network error");
        assert.includes(res.toString(), "network error while attempting to run command");
    }, rollbackNode.port);

    const cursorIdToBeReadDuringRollback = assert
                                               .commandWorked(rollbackNode.getDB(dbName).runCommand(
                                                   {"find": sourceCollName, batchSize: 0}))
                                               .cursor.id;

    jsTestLog("RollbackOps");
    RollbackOps(rollbackNode);

    // Wait for getMore to hang
    checkLog.contains(rollbackNode, "GetMoreHangBeforeReadlock fail point enabled.");

    // Start rollback
    jsTestLog("transitionToSyncSourceOperationsBeforeRollback");
    rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
    jsTestLog("transitionToSyncSourceOperationsDuringRollback");
    rollbackTest.transitionToSyncSourceOperationsDuringRollback();

    jsTestLog("Reconnecting");
    reconnect(rollbackNode.getDB(dbName));

    // Wait for rollback to hang
    checkLog.contains(rollbackNode, "rollbackHangAfterTransitionToRollback fail point enabled.");

    ClearFailPoint(rollbackNode, "GetMoreHangBeforeReadlock");

    jsTestLog("Wait for getmore thread to join");
    joinGetMoreThread();

    jsTestLog("Reading during rollback");
    findShouldFailDuringRollback(rollbackNode);
    getMoreShouldFailDuringRollback(rollbackNode, cursorIdToBeReadDuringRollback);

    SetFailPoint(rollbackNode, "skipCheckingForNotMasterInCommandDispatch");
    jsTestLog("Reading during rollback (skip checks in service_entry_point_common.cpp)");
    findShouldFailDuringRollback(rollbackNode);
    getMoreShouldFailDuringRollback(rollbackNode, cursorIdToBeReadDuringRollback);

    ClearFailPoint(rollbackNode, "rollbackHangAfterTransitionToRollback");

    rollbackTest.transitionToSteadyStateOperations();

    // Check the replica set.
    rollbackTest.stop();

    // Check if any tests failed
    assert.eq(pass, 1);
}());
