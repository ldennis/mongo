// Tests multi-document transactions metrics are still correct after 'killSessions'.
// @tags: [uses_transactions]
(function() {
    "use strict";

    // Verifies that the given value of the transaction metrics is incremented in the way we expect.
    function verifyMetricsChange(initialStats, newStats, valueName, expectedIncrement) {
        assert.eq(initialStats[valueName] + expectedIncrement,
                  newStats[valueName],
                  "expected " + valueName + " to increase by " + expectedIncrement +
                      ".\nInitial stats: " + tojson(initialStats) + "; New stats: " +
                      tojson(newStats));
    }

    // Set up the replica set.
    const rst = new ReplSetTest({nodes: 1, nodeOptions: {enableMajorityReadConcern: "true"}});
    rst.startSet();
    rst.initiate();

    // Set up the test database.
    const dbName = "test";
    const collName = "server_transactions_metrics_kill_sessions";
    const testDB = rst.getPrimary().getDB(dbName);
    testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
    assert.commandWorked(testDB.runCommand({create: collName, writeConcern: {w: "majority"}}));

    // Start the session.
    const sessionOptions = {causalConsistency: false};
    let session = testDB.getMongo().startSession(sessionOptions);
    let sessionDb = session.getDatabase(dbName);

    let initialMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;

    jsTest.log("Start a transaction.");
    session.startTransaction();
    assert.commandWorked(sessionDb.runCommand({find: collName}));

    let newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
    verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 1);
    verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 1);

    jsTest.log("Kill session " + tojson(session.getSessionId()) + ".");
    assert.commandWorked(testDB.runCommand({killSessions: [session.getSessionId()]}));

    newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
    verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "totalCommitted", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "totalAborted", 1);
    verifyMetricsChange(initialMetrics, newMetrics, "totalStarted", 1);

    // End the session.
    session.endSession();

    session = testDB.getMongo().startSession(sessionOptions);
    sessionDb = session.getDatabase(dbName);

    jsTest.log("Start a snapshot transaction at a time that is too old.");
    session.startTransaction({readConcern: {level: "snapshot", atClusterTime: Timestamp(1, 1)}});
    // Operation runs unstashTransactionResources() and throws prior to onUnstash().
    assert.commandFailedWithCode(sessionDb.runCommand({find: collName}), ErrorCodes.SnapshotTooOld);

    newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
    verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 1);
    verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 1);

    // Kill the session that threw exception before.
    jsTest.log("Kill session " + tojson(session.getSessionId()) + ".");
    assert.commandWorked(testDB.runCommand({killSessions: [session.getSessionId()]}));

    newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
    verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "totalCommitted", 0);
    verifyMetricsChange(initialMetrics, newMetrics, "totalAborted", 2);
    verifyMetricsChange(initialMetrics, newMetrics, "totalStarted", 2);

    // End the session.
    session.endSession();

    // Stop the replica set.
    rst.stopSet();
}());
