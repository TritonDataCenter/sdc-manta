/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.oneach_formatters.js: tests manta-oneach output formatters.  This test
 * just emits a bunch of output, and the test runner (catest) is responsible for
 * checking that output against the expected output.
 */

var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var vasync = require('vasync');

var oneach = require('../lib/oneach/oneach');

var event_sets = {
    'empty': [],
    'auto-oneline': [ {
	'uuid': 'my_uuid',
	'hostname': 'garbage',
	'zonename': 'chunkify',
	'service': 'webapi',
	'result': {
	    'exit_status': 0,
	    'stdout': 'hello world\n',
	    'stderr': ''
	}
    } ],
    'auto-multiline-stderr': [ {
	'uuid': 'my_uuid',
	'hostname': 'garbage',
	'zonename': 'chunkify',
	'service': 'webapi',
	'result': {
	    'exit_status': 1,
	    'stdout': 'hello world\n',
	    'stderr': 'line one\nline two\nline three\n'
	}
    } ],
    'auto-multiline': [ {
	'uuid': 'my_uuid',
	'hostname': 'garbage',
	'zonename': 'chunkify',
	'service': 'webapi',
	'result': {
	    'exit_status': 0,
	    'stdout': 'hello world\n',
	    'stderr': ''
	}
    }, {
	'uuid': 'my_uuid_two',
	'hostname': 'trash',
	'zonename': 'prettify',
	'service': 'moray',
	'result': {
	    'exit_status': 0,
	    'stdout': 'hello world\nI have a second line\n',
	    'stderr': 'stderr should normally be ignored\n'
	}
    }, {
	'uuid': 'my_uuid_two',
	'hostname': 'trash',
	'zonename': 'beautify',
	'service': 'postgres',
	'result': {
	    'exit_status': 1,
	    'stdout': '',
	    'stderr': 'stderr is not ignored here\n'
	}
    } ]
};

var test_cases = [ {
    'event_set': 'empty',
    'formatter': new oneach.mzResultToJson()
}, {
    'event_set': 'auto-multiline-stderr',
    'formatter': new oneach.mzResultToJson()
}, {
    'event_set': 'auto-multiline',
    'formatter': new oneach.mzResultToJson()
}, {
    'event_set': 'empty',
    'formatter': new oneach.mzResultToText({
	'omitHeader': true,
	'outputBatch': false,
	'multilineMode': 'one'
    })
}, {
    'event_set': 'auto-multiline',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': false,
	'multilineMode': 'multi'
    })
}, {
    'event_set': 'auto-multiline',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': false,
	'multilineMode': 'one'
    })
}, {
    'event_set': 'auto-multiline',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': true,
	'multilineMode': 'multi'
    })
}, {
    'event_set': 'auto-multiline',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': true,
	'multilineMode': 'one'
    })
}, {
    'event_set': 'auto-multiline-stderr',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': true,
	'multilineMode': 'auto'
    })
}, {
    'event_set': 'auto-multiline',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': true,
	'multilineMode': 'auto'
    })
}, {
    'event_set': 'auto-oneline',
    'formatter': new oneach.mzResultToText({
	'omitHeader': false,
	'outputBatch': true,
	'multilineMode': 'auto'
    })
} ];

var nexecuted = 0;

function main()
{
	vasync.forEachPipeline({
	    'inputs': test_cases,
	    'func': runTestCase
	}, function (err) {
		assertplus.equal(nexecuted, test_cases.length);
		console.error('%d test case%s executed',
		    nexecuted, nexecuted == 1 ? '' : 's');
	});

	process.on('exit', function () {
		assertplus.equal(nexecuted, test_cases.length,
		    'premature exit');
	});
}

function runTestCase(testcase, callback)
{
	var formatter, events;

	assertplus.object(testcase);
	assertplus.string(testcase['event_set']);
	assertplus.ok(event_sets.hasOwnProperty(testcase['event_set']));
	assertplus.object(testcase['formatter']);

	console.log('TEST CASE: set %s, formatter %s',
	    testcase['event_set'], testcase['formatter'].constructor.name);
	console.log('---------------------------------');
	nexecuted++;

	formatter = testcase['formatter'];
	formatter.pipe(process.stdout, { 'end': false });
	formatter.on('end', function () {
		console.log('---------------------------------');
		callback();
	});

	events = jsprim.deepCopy(event_sets[testcase['event_set']]);
	events.forEach(function (e) { formatter.write(e); });
	formatter.end();
}

main();
