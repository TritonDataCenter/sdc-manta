/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.oneach_cmd.js: integration test that actually runs the "manta-oneach"
 * command to test basic cases.  Much of the underlying functionality is tested
 * by unit tests, so this is mostly a smoke test for the tool itself.
 */

var assert = require('assert');
var cmdutil = require('cmdutil');
var forkexec = require('forkexec');
var path = require('path');
var vasync = require('vasync');
var VError = require('verror').VError;

var execname = path.join(__dirname, '../bin/manta-oneach');
var nstarted = 0;
var ndone = 0;

function main()
{
	var testcases = [
	    testCaseBadScope,
	    testCaseMissingArg,
	    testCaseAllZones,
	    testCaseGlobalZonesJson,
	    testCaseFilter
	];

	vasync.waterfall(testcases, function (err) {
		if (err) {
			cmdutil.fail(err);
		}

		assert.equal(nstarted, testcases.length);
		assert.equal(nstarted, ndone);
	});
}

function assertNoError(name, err)
{
	if (err) {
		cmdutil.fail(new VError(err,
		    'test case "%s": unexpected error', name));
	}
}

function assertEmptyStderr(name, result)
{
	if (result['stderr'].length !== 0) {
		cmdutil.fail(new VError(
		    'test case "%s": expected empty stderr, found:\n%s\n',
		    name, result['stderr']));
	}
}

function testCaseStart(name)
{
	console.error('test case: %s', name);
	nstarted++;
}

/*
 * Test case: invalid combination of scopes
 */
function testCaseBadScope(callback)
{
	var name = 'manta-oneach -a -z asdf date';

	testCaseStart(name);
	forkexec.forkExecWait({
	    'argv': [ process.execPath, execname, '-a', '-z', 'asdf', 'date' ]
	}, function (err, result) {
		assert.ok(err);
		assert.equal(result['status'], 2);
		assert.ok(new RegExp('^manta-oneach: cannot specify zones' +
		    '.*when all zones were requested').test(result['stderr']));
		ndone++;
		callback();
	});
}


/*
 * Test case: node-getopt error
 */
function testCaseMissingArg(callback)
{
	var name = 'manta-oneach -z';

	testCaseStart(name);
	forkexec.forkExecWait({
	    'argv': [ process.execPath, execname, '-z' ]
	}, function (err, result) {
		assert.ok(err);
		assert.equal(result['status'], 2);
		assert.ok(/^option requires an argument -- z/.test(
		    result['stderr']));
		ndone++;
		callback();
	});
}

/*
 * Test case: sample output from all zones.
 */
function testCaseAllZones(callback)
{
	var name = 'manta-oneach -a zonename';

	testCaseStart(name);
	forkexec.forkExecWait({
	    'argv': [ process.execPath, execname, '-a', 'zonename' ]
	}, function (err, result) {
		assertNoError(name, err);
		assertEmptyStderr(name, result);

		var lines, i, parts;
		lines = result.stdout.split('\n');
		assert.ok(lines[lines.length - 1].length === 0,
		    'last line of output should have been empty');
		/* Skip header line and last (blank) line. */
		for (i = 1; i < lines.length - 1; i++) {
			parts = lines[i].split(/\s+/);
			assert.equal(parts.length, 3,
			    'line ' + (i + 1) + ' garbled');
			assert.equal(parts[1], parts[2].substr(0, 8));
		}

		ndone++;
		callback();
	});
}

/*
 * Test case: using global zones
 */
function testCaseGlobalZonesJson(callback)
{
	var name = 'manta-oneach -GJa "sysinfo | json UUID"';

	testCaseStart(name);
	forkexec.forkExecWait({
	    'argv': [ process.execPath, execname, '-GJa',
	        'sysinfo | json UUID' ]
	}, function (err, result) {
		assertNoError(name, err);
		assertEmptyStderr(name, result);

		var lines, i, parsed;
		lines = result.stdout.split('\n');
		assert.ok(lines[lines.length - 1].length === 0,
		    'last line of output should have been empty');

		/* Skip the last (blank) line. */
		for (i = 1; i < lines.length - 1; i++) {
			parsed = JSON.parse(lines[i]);
			assert.ok(parsed['uuid']);
			assert.equal(parsed['uuid'] + '\n',
			    parsed['result']['stdout']);
			assert.equal(parsed['result']['exit_status'], 0);
			assert.equal(parsed['result']['stderr'], '');
			assert.ok(parsed['hostname']);
		}

		ndone++;
		callback();
	});
}

/*
 * Test case: filtering zones
 */
function testCaseFilter(callback)
{
	var name = 'manta-oneach -s postgres "svcs -H -o fmri manatee-sitter"';

	testCaseStart(name);
	forkexec.forkExecWait({
	    'argv': [ process.execPath, execname, '-s', 'postgres',
	        'svcs -H -o fmri manatee-sitter' ]
	}, function (err, result) {
		assertNoError(name, err);
		assertEmptyStderr(name, result);

		var lines, i, parts;
		lines = result.stdout.split('\n');
		assert.ok(lines[lines.length - 1].length === 0,
		    'last line of output should have been empty');
		/* Skip header line and last (blank) line. */
		for (i = 1; i < lines.length - 1; i++) {
			parts = lines[i].split(/\s+/);
			assert.equal(parts.length, 3,
			    'line ' + (i + 1) + ' garbled');
			assert.equal(parts[0], 'postgres');
			assert.equal(parts[2],
			    'svc:/manta/application/manatee-sitter:default');
		}

		ndone++;
		callback();
	});
}

main();
