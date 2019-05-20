/**
 * This file tests commands that do not support write concern. It passes both valid and invalid
 * writeConcern fields to commands and expects the commands to fail with a writeConcernNotSupported
 * error.
 *
 * @tags: [
 *   assumes_write_concern_unchanged,
 *   # mapReduce does not support afterClusterTime.
 *   does_not_support_causal_consistency,
 *   does_not_support_stepdowns,
 * ]
 */

(function() {
    "use strict";
    var collName = 'leaves';

    var commands = [];

    commands.push({count: collName, query: {type: 'oak'}});

    commands.push({aggregate: collName, pipeline: [{$sort: {type: 1}}], cursor: {}});

    commands.push({
        mapReduce: collName,
        map: function() {
            this.tags.forEach(function(z) {
                emit(z, 1);
            });
        },
        reduce: function(key, values) {
            return {count: values.length};
        },
        out: {inline: 1}
    });

    function assertWriteConcernNotSupportedError(res) {
        assert.commandFailed(res);
        assert.eq(res.code, ErrorCodes.InvalidOptions);
        assert(!res.writeConcernError);
    }

    // Test a variety of valid and invalid writeConcerns to confirm that they still all get
    // the correct error.
    var writeConcerns = [{w: 'invalid'}, {w: 1}];

    function testUnsupportedWriteConcern(wc, cmd) {
        cmd.writeConcern = wc;
        jsTest.log("Testing " + tojson(cmd));

        var res = db.runCommand(cmd);
        assertWriteConcernNotSupportedError(res);
    }

    // Verify that each command gets a writeConcernNotSupported error.
    commands.forEach(function(cmd) {
        writeConcerns.forEach(function(wc) {
            testUnsupportedWriteConcern(wc, cmd);
        });
    });
})();
