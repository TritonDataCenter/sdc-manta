/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.oneach_cli.js: tests manta-oneach command-line argument parsing
 */

var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var oneach = require('../lib/oneach/oneach');
var oneach_cli = require('../lib/oneach/cli');

var testcommon = require('./common');

/*
 * Each test case has input command-line arguments "argv" and either "error"
 * (describing the expected error message) or "checkFields" (describing the set
 * of properties in the returned object that differ from their default values).
 */
var test_cases = [
/* Syntactic issues */
{
    'name': 'no arguments',
    'argv': [],
    'error': /expected command/
}, {
    'name': 'missing scope',
    'argv': [ 'date' ],
    'error': /must explicitly request all zones/
}, {
    'name': 'bad option',
    'argv': [ '-Q' ],
    'error': /node-getopt error/
}, {
    'name': 'missing required option argument',
    'argv': [ '-z' ],
    'error': /node-getopt error/
},

/* Semantic issues */
{
    'name': 'conflicting scopes (all + zone)',
    'argv': [ '-a', '-z', 'asdf', 'date' ],
    'error': /cannot specify zones, services, or compute nodes when all zones/
}, {
    'name': 'conflicting scopes (all + service)',
    'argv': [ '-a', '-s', 'asdf', 'date' ],
    'error': /cannot specify zones, services, or compute nodes when all zones/
}, {
    'name': 'conflicting scopes (all + compute node)',
    'argv': [ '-a', '-S', 'asdf', 'date' ],
    'error': /cannot specify zones, services, or compute nodes when all zones/
}, {
    'name': 'bad argument for --amqp-port (non-numeric)',
    'argv': [ '--amqp-port', 'Q123', '-a', 'date' ],
    'error': /expected positive integer for --amqp-port, but got: Q123/
}, {
    'name': 'bad argument for --amqp-port (zero)',
    'argv': [ '--amqp-port', '0', '-a', 'date' ],
    'error': /expected positive integer for --amqp-port, but got: 0/
}, {
    'name': 'bad argument for --amqp-port (negative)',
    'argv': [ '--amqp-port=-3', '-a', 'date' ],
    'error': /expected positive integer for --amqp-port, but got: -3/
}, {
    'name': 'bad argument for --amqp-timeout (non-numeric)',
    'argv': [ '--amqp-timeout', 'Q123', '-a', 'date' ],
    'error': /expected positive integer for --amqp-timeout, but got: Q123/
}, {
    'name': 'bad argument for --amqp-timeout (zero)',
    'argv': [ '--amqp-timeout', '0', '-a', 'date' ],
    'error': /expected positive integer for --amqp-timeout, but got: 0/
}, {
    'name': 'bad argument for --amqp-timeout (negative)',
    'argv': [ '--amqp-timeout=-3', '-a', 'date' ],
    'error': /expected positive integer for --amqp-timeout, but got: -3/
}, {
    'name': 'bad argument for --concurrency (non-numeric)',
    'argv': [ '--concurrency', 'Q123', '-a', 'date' ],
    'error': /expected positive integer for -c\/--concurrency, but got: Q123/
}, {
    'name': 'bad argument for --concurrency (zero)',
    'argv': [ '--concurrency', '0', '-a', 'date' ],
    'error': /expected positive integer for -c\/--concurrency, but got: 0/
}, {
    'name': 'bad argument for --concurrency (negative)',
    'argv': [ '--concurrency=-3', '-a', 'date' ],
    'error': /expected positive integer for -c\/--concurrency, but got: -3/
}, {
    'name': 'bad argument for --exec-timeout (non-numeric)',
    'argv': [ '--exectimeout', 'Q123', '-a', 'date' ],
    'error': /expected positive integer for -T\/--exectimeout, but got: Q123/
}, {
    'name': 'bad argument for --exec-timeout (zero)',
    'argv': [ '--exectimeout', '0', '-a', 'date' ],
    'error': /expected positive integer for -T\/--exectimeout, but got: 0/
}, {
    'name': 'bad argument for --exec-timeout (negative)',
    'argv': [ '--exectimeout=-3', '-a', 'date' ],
    'error': /expected positive integer for -T\/--exectimeout, but got: -3/
},

/* Working examples: exercising scopes */
{
    'name': 'simple command, with spaces, on all nodes',
    'argv': [ '-a', 'ls -l /var/tmp' ],
    'checkFields': {
	'scopeAllZones': true,
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'ls -l /var/tmp'
    }
}, {
    'name': 'simple command, on all nodes, globals',
    'argv': [ '--all-zones', '-G', 'date' ],
    'checkFields': {
	'scopeAllZones': true,
	'scopeGlobalZones': true,
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date'
    }
}, {
    'name': 'simple scope: one zone, two arguments',
    'argv': [ '-z', 'zone1', 'date' ],
    'checkFields': {
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date',
	'scopeZones': [ 'zone1' ]
    }
}, {
    'name': 'simple scope: one zone, one argument',
    'argv': [ '-zzone1', 'date' ],
    'checkFields': {
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date',
	'scopeZones': [ 'zone1' ]
    }
}, {
    'name': 'simple scope: complex -z arguments, global zones',
    'argv': [ '-zzone1,zone2,zone3', '-z', 'zone4', '--global-zones',
	'-zzone5,zone6', 'date' ],
    'checkFields': {
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date',
	'scopeGlobalZones': true,
	'scopeZones': [ 'zone1', 'zone2', 'zone3', 'zone4', 'zone5', 'zone6' ]
    }
}, {
    'name': 'complex scope',
    'argv': [
	'--zonename=z1', '--service=s1,s2', '--zonename=z2,z3',
	'--service=s3', '-s', 's4', '-S', 'H1', '--compute-node', 'H2,H3',
	'date'
    ],
    'checkFields': {
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date',
	'scopeZones': [ 'z1', 'z2', 'z3' ],
	'scopeServices': [ 's1', 's2', 's3', 's4' ],
	'scopeComputeNodes': [ 'H1', 'H2', 'H3' ]
    }
},

/* Working examples: exercising miscellaneous options */
{
    'name': 'miscellaneous short options and a complex bash script',
    'argv': [ '-c', '3', '-I', '-J', '-N', '-T', '7', '-X', '-a',
	'date && foo > bar & sleep 3 < /dev/null' ],
    'checkFields': {
	'concurrency': 3,
	'outputBatch': false,
	'outputMode': 'jsonstream',
	'multilineMode': 'one',
	'execTimeout': 7000,
	'execClobber': true,
	'scopeAllZones': true,
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date && foo > bar & sleep 3 < /dev/null'
    }
}, {
    'name': 'miscellaneous long options',
    'argv': [
	'--amqp-host=MY_AMQP_HOST', '--amqp-port=1234', '--amqp-login=HALO',
	'--amqp-password=LETMEIN', '--amqp-timeout=3',
	'--concurrency', '3', '--dry-run', '--immediate',
	'--jsonstream', '--oneline', '--exectimeout', '7',
	'--clobber', '-a', 'date' ],
    'checkFields': {
	'amqpHost': 'MY_AMQP_HOST',
	'amqpPort': 1234,
	'amqpLogin': 'HALO',
	'amqpPassword': 'LETMEIN',
	'amqpTimeout': 3000,
	'concurrency': 3,
	'dryRun': true,
	'outputBatch': false,
	'outputMode': 'jsonstream',
	'multilineMode': 'one',
	'execTimeout': 7000,
	'execClobber': true,
	'scopeAllZones': true,
	'execMode': oneach.MZ_EM_COMMAND,
	'execCommand': 'date'
    }
},

/* File transfers: bad arguments */
{
    'name': 'using -g without --dir',
    'argv': [ '-g', '/garbage/in', '-a' ],
    'error': /--dir is required with --put and --get/
}, {
    'name': 'using -p without --dir',
    'argv': [ '-p', '/garbage/can', '-a' ],
    'error': /--dir is required with --put and --get/
}, {
    'name': 'using -d with a command',
    'argv': [ '-d', '/put/your', '-a', 'wat' ],
    'error': /--dir cannot be used without --put or --get/
}, {
    'name': 'file transfer with extra arguments',
    'argv': [ '--dir', '/mydir', '--put', '/asdf', '-a', 'OBEY' ],
    'error': /unexpected arguments/
},

/* File transfers: valid arguments */
{
    'name': 'file transfer: simple put',
    'argv': [ '--dir', '/mydir', '--put', '/asdf', '-a' ],
    'checkFields': {
	'execMode': oneach.MZ_EM_RECEIVEFROMREMOTE,
	'execDirectory': '/mydir',
	'execFile': '/asdf',
	'scopeAllZones': true
    }
}, {
    'name': 'file transfer: complex get',
    'argv': [ '--dir', '/mydir', '--get', '/asdf', '-z', 'es6' ],
    'checkFields': {
	'execMode': oneach.MZ_EM_SENDTOREMOTE,
	'execDirectory': '/mydir',
	'execFile': '/asdf',
	'execClobber': false,
	'scopeZones': [ 'es6' ]
    }
}, {
    'name': 'file transfer: get with clobber',
    'argv': [ '--dir', '/mydir', '--get', '/asdf', '-z', 'es6', '-X' ],
    'checkFields': {
	'execMode': oneach.MZ_EM_SENDTOREMOTE,
	'execDirectory': '/mydir',
	'execFile': '/asdf',
	'execClobber': true,
	'scopeZones': [ 'es6' ]
    }
} ];

var nexecuted = 0;
var nerrors = 0;

function main()
{
	test_cases.forEach(runTestCase);
	assertplus.equal(nexecuted, test_cases.length);
	console.error('%d test case%s executed, %d failure%s',
	    nexecuted, nexecuted == 1 ? '' : 's',
	    nerrors, nerrors == 1 ? '' : 's');
	process.exit(nerrors === 0 ? 0 : 1);
}

function runTestCase(testcase)
{
	var expected_err, expected_rv, actual;

	assertplus.object(testcase);
	assertplus.string(testcase['name']);
	assertplus.arrayOfString(testcase['argv']);

	if (testcase.hasOwnProperty('error')) {
		assertplus.ok(!testcase.hasOwnProperty('checkFields'));
		expected_err = testcase['error'];
		assertplus.ok(expected_err !== null);
		expected_rv = null;
	} else if (testcase.hasOwnProperty('checkFields')) {
		assertplus.ok(!testcase.hasOwnProperty('error'));
		expected_err = null;
		expected_rv = testcommon.defaultCommandExecutorArgs();
		jsprim.forEachKey(testcase['checkFields'], function (k, v) {
			expected_rv[k] = v;
		});
	}

	console.error('TEST CASE: %s', testcase['name']);
	actual = oneach_cli.mzParseCommandLine(testcase['argv']);
	nexecuted++;

	if (actual === null) {
		/* node-getopt emitted an error to stderr.  Fake one up here. */
		actual = new Error('node-getopt error');
	}

	if (actual instanceof Error) {
		if (expected_err === null) {
			console.error('expected fields: ',
			    testcase['checkFields']);
			console.error('found unexpected error: ',
			    actual);
			nerrors++;
		} else if (!expected_err.test(actual.message)) {
			console.error('expected error: ', expected_err);
			console.error('found error: ', actual.message);
			nerrors++;
		}
	} else {
		/*
		 * streamStatus is a circular object, and it doesn't depend on
		 * the arguments anyway, so just remove it.
		 */
		assertplus.ok(actual.hasOwnProperty('streamStatus'));
		assertplus.ok(actual['streamStatus'] == process.stderr);
		delete (actual['streamStatus']);

		if (expected_err !== null) {
			console.error('expected error: ', expected_err);
			console.error('found non-error: ', actual);
			nerrors++;
		} else if (!jsprim.deepEqual(expected_rv, actual)) {
			console.error('expected: ', expected_rv);
			console.error('actual: ', actual);
			nerrors++;
		}
	}
}

main();
