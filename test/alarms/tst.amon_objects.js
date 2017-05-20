/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.amon_objects.js: tests for Amon object schema and representations
 */

var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var VError = require('verror');

var amon_objects = require('../../lib/alarms/amon_objects');

var testCases;

function main()
{
	generateTestCases();
	testCases.forEach(runTestCase);
	console.error('%s okay', __filename);
}

/*
 * Each test case must have properties:
 *
 *    name (string)	name of the test case
 *
 *    objtype (string)	one of "probe", "probe group", or "alarm"
 *
 *    input (object)	an object as it might be returned from Amon
 *
 * plus exactly one of these properties:
 *
 *    errmsg (regexp)	a regular expression to be checked against an error
 *    			resulting from loading the given input object
 *
 *    verify (func)	a check function to verify the loaded object
 */
function runTestCase(tc)
{
	var result, load, errpattern;

	assertplus.object(tc, 'tc');
	assertplus.string(tc.name, 'tc.name');
	process.stderr.write('test case: ' + tc.name + ': ');
	if (tc.input !== null) {
		assertplus.object(tc.input, 'tc.input');
	}

	if (tc.hasOwnProperty('errmsg')) {
		assertplus.ok(!tc.hasOwnProperty('verify'));
		errpattern = tc.errmsg;
	} else {
		assertplus.func(tc.verify, 'tc.verify');
		errpattern = null;
	}

	switch (tc.objtype) {
	case 'alarm':
		load = amon_objects.loadAlarmObject;
		break;

	case 'probe':
		load = amon_objects.loadProbeObject;
		break;

	case 'probegroup':
		load = amon_objects.loadProbeGroupObject;
		break;

	default:
		throw (new VError('unsupported object type: "%s"', tc.objtype));
	}

	result = load(tc.input);
	if (result instanceof Error) {
		if (errpattern === null) {
			throw (new VError(result,
			    'expected no error, but found one'));
		}

		if (!errpattern.test(result.message)) {
			throw (new VError(result,
			    'expected error %s, but found',
			    JSON.stringify(errpattern.source)));
		}

		console.error('okay (found expected error)');
	} else {
		tc.verify(result);
		console.error('okay (verified result)');
	}
}

function generateTestCases()
{
	var validAlarm, validProbeGroup, validProbe, input;

	testCases = [];

	/*
	 * We'll start with this basic alarm and verify that it gets loaded
	 * correctly.  Then we'll transform it in various ways to make it
	 * invalid and make sure that we get the appropriate error message.
	 */
	validAlarm = {
	    'id': 791,
	    'user': 'martin',
	    'closed': false,
	    'suppressed': false,
	    'timeOpened': Date.parse('2017-04-25T01:23:45.678Z'),
	    'timeClosed': null,
	    'timeLastEvent': Date.parse('2017-04-26T09:54:32.876Z'),
	    'numEvents': 14,
	    'faults': [ {
		'type': 'probe',
		'probe': 'probe-uuid-1',
		'event': {
		    'v': 1,
		    'type': 'probe',
		    'clear': false,
		    'machine': 'machine-uuid-1',
		    'uuid': 'event-uuid-1',
		    'agent': 'agent-uuid-1',
		    'agentAlias': 'anAgent1',
		    'time': Date.parse('2017-04-26T09:54:32.876Z'),
		    'data': {
			'message': 'some summary message'
		    }
		}
	    }, {
		'type': 'probe',
		'probe': 'probe-uuid-2',
		'event': {
		    'v': 1,
		    'type': 'probe',
		    'clear': true,
		    'machine': 'machine-uuid-2',
		    'uuid': 'event-uuid-2',
		    'agent': 'agent-uuid-2',
		    'agentAlias': 'anAgent2',
		    'time': Date.parse('2017-04-26T08:54:32.876Z'),
		    'data': {
			'message': 'a different summary message'
		    }
		}
	    } ]
	};

	testCases = [ {
	    'name': 'alarm: basic case',
	    'objtype': 'alarm',
	    'input': validAlarm,
	    'verify': function verifyBasicAlarm(result) {
		var fault, extras;

		assertplus.strictEqual(result.a_id, 791);
		assertplus.strictEqual(result.a_user, 'martin');
		assertplus.strictEqual(result.a_groupid, null);
		assertplus.strictEqual(result.a_closed, false);
		assertplus.strictEqual(result.a_suppressed, false);
		assertplus.strictEqual(result.a_time_opened.toISOString(),
		    '2017-04-25T01:23:45.678Z');
		assertplus.strictEqual(result.a_time_closed, null);
		assertplus.strictEqual(result.a_time_last.toISOString(),
		    '2017-04-26T09:54:32.876Z');
		assertplus.strictEqual(result.a_nevents, 14);
		assertplus.arrayOfObject(result.a_faults);
		assertplus.strictEqual(result.a_faults.length, 2);

		/*
		 * If new properties are added to alarm objects, they should be
		 * tested above and added to the list below.
		 */
		extras = jsprim.extraProperties(result, [
		    'a_id',
		    'a_user',
		    'a_groupid',
		    'a_closed',
		    'a_suppressed',
		    'a_time_opened',
		    'a_time_closed',
		    'a_time_last',
		    'a_nevents',
		    'a_faults'
		]);
		assertplus.deepEqual([], extras,
		    'alarm object has untested properties: ' +
		    extras.join(','));

		fault = result.a_faults[0];
		assertplus.ok(fault.aflt_alarm == result);
		assertplus.strictEqual(fault.aflt_probeid, 'probe-uuid-1');
		assertplus.strictEqual(fault.aflt_clear, false);
		assertplus.strictEqual(fault.aflt_uuid, 'event-uuid-1');
		assertplus.strictEqual(fault.aflt_machine, 'machine-uuid-1');
		assertplus.strictEqual(fault.aflt_agent, 'agent-uuid-1');
		assertplus.strictEqual(fault.aflt_agent_alias, 'anAgent1');
		assertplus.strictEqual(fault.aflt_time.toISOString(),
		    '2017-04-26T09:54:32.876Z');
		assertplus.strictEqual(fault.aflt_summary,
		    'some summary message');
		assertplus.object(fault.aflt_data);

		/*
		 * Similarly, if new properties are added to fault objects,
		 * they should be tested above and added to the list below.
		 */
		extras = jsprim.extraProperties(fault, [
		    'aflt_alarm',
		    'aflt_probeid',
		    'aflt_clear',
		    'aflt_uuid',
		    'aflt_machine',
		    'aflt_agent',
		    'aflt_agent_alias',
		    'aflt_time',
		    'aflt_summary',
		    'aflt_data'
		]);
		assertplus.deepEqual([], extras,
		    'fault object has untested properties: ' +
		    extras.join(','));

		fault = result.a_faults[1];
		assertplus.ok(fault.aflt_alarm == result);
		assertplus.strictEqual(fault.aflt_probeid, 'probe-uuid-2');
		assertplus.strictEqual(fault.aflt_clear, true);
		assertplus.strictEqual(fault.aflt_uuid, 'event-uuid-2');
		assertplus.strictEqual(fault.aflt_machine, 'machine-uuid-2');
		assertplus.strictEqual(fault.aflt_agent, 'agent-uuid-2');
		assertplus.strictEqual(fault.aflt_agent_alias, 'anAgent2');
		assertplus.strictEqual(fault.aflt_time.toISOString(),
		    '2017-04-26T08:54:32.876Z');
		assertplus.strictEqual(fault.aflt_summary,
		    'a different summary message');
	    }
	} ];

	/*
	 * Exercise different values for a valid alarm.
	 */
	input = jsprim.deepCopy(validAlarm);
	input.id = 0;
	input.suppressed = true;
	input.closed = true;
	input.timeClosed = Date.parse('2017-04-27T00:00:00.123Z');
	testCases.push({
	    'name': 'alarm: closed and suppressed',
	    'objtype': 'alarm',
	    'input': input,
	    'verify': function (result) {
		assertplus.strictEqual(result.a_id, 0);
		assertplus.strictEqual(result.a_suppressed, true);
		assertplus.strictEqual(result.a_closed, true);
		assertplus.strictEqual(result.a_time_closed.toISOString(),
		    '2017-04-27T00:00:00.123Z');
	    }
	});

	/*
	 * For each required property, create a test case that exercises what
	 * happens when that property is missing.
	 */
	[ 'id', 'user', 'closed', 'suppressed', 'timeOpened', 'timeClosed',
	    'timeLastEvent', 'numEvents', 'faults' ].forEach(function (prop) {
		input = jsprim.deepCopy(validAlarm);
		delete (input[prop]);
		testCases.push({
		    'name': 'alarm: missing "' + prop + '"',
		    'objtype': 'alarm',
		    'input': input,
		    'errmsg': new RegExp(
		        'property "' + prop + '": .* missing and.*required')
		});
	});

	/*
	 * "Bad type" test cases.
	 */

	input = jsprim.deepCopy(validAlarm);
	input.id = 'one-two-three';
	testCases.push({
	    'name': 'alarm: bad "id"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /^property "id": string value found.*integer.*required$/
	});

	input = jsprim.deepCopy(validAlarm);
	input.user = 47;
	testCases.push({
	    'name': 'alarm: bad "user"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "user":.*number.*found.*string.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.user = 47;
	testCases.push({
	    'name': 'alarm: bad "user"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "user":.*number.*found.*string.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.probeGroup = {};
	testCases.push({
	    'name': 'alarm: bad "probeGroup"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "probeGroup":.*object.*found.*string.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.closed = {};
	testCases.push({
	    'name': 'alarm: bad "closed"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "closed":.*object.*found.*bool.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.suppressed = {};
	testCases.push({
	    'name': 'alarm: bad "suppressed"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "suppressed":.*object.*found.*bool.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.timeOpened = '2017-04-25T01:23:45.678Z';
	testCases.push({
	    'name': 'alarm: bad "timeOpened"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "timeOpened":.*string.*found.*number.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.timeClosed = '2017-04-25T01:23:45.678Z';
	testCases.push({
	    'name': 'alarm: bad "timeClosed"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "timeClosed":.*string.*found.*number.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.timeLastEvent = '2017-04-25T01:23:45.678Z';
	testCases.push({
	    'name': 'alarm: bad "timeLastEvent"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /"timeLastEvent":.*string.*found.*number.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.numEvents = 'forty-seven';
	testCases.push({
	    'name': 'alarm: bad "numEvents"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "numEvents":.*string.*found.*integer.*required/
	});

	input = jsprim.deepCopy(validAlarm);
	input.faults = 'boom!';
	testCases.push({
	    'name': 'alarm: bad "faults"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "faults":.*string.*found.*array.*required/
	});

	/*
	 * Bad fault objects: missing required properties.
	 */

	[ 'type', 'probe', 'event' ].forEach(function (prop) {
		input = jsprim.deepCopy(validAlarm);
		delete (input.faults[0][prop]);
		testCases.push({
		    'name': 'fault: missing property "' + prop + '"',
		    'objtype': 'alarm',
		    'input': input,
		    'errmsg': new RegExp('property "faults\\[0\\].' + prop +
		        '".*missing.*required')
		});
	});

	[ 'v', 'type', 'clear', 'machine', 'uuid', 'agent', 'agentAlias',
	    'time', 'data' ].forEach(function (prop) {
		input = jsprim.deepCopy(validAlarm);
		delete (input.faults[0].event[prop]);
		testCases.push({
		    'name': 'fault: missing property "event.' + prop + '"',
		    'objtype': 'alarm',
		    'input': input,
		    'errmsg': new RegExp('property "faults\\[0\\].event.' +
		        prop + '".*missing.*required')
		});
	});

	/*
	 * Bad fault objects: bad types for various properties.
	 */

	[ 'type', 'probe', 'event' ].forEach(function (prop) {
		input = jsprim.deepCopy(validAlarm);
		input.faults[0][prop] = 17;
		testCases.push({
		    'name': 'fault: bad "' + prop + '"',
		    'objtype': 'alarm',
		    'input': input,
		    'errmsg': new RegExp('property "faults\\[0\\].' + prop +
		        '": number value found.*required')
		});
	});

	[ 'v', 'type', 'clear', 'machine', 'uuid', 'agent', 'agentAlias',
	    'time' ].forEach(function (prop) {
		input = jsprim.deepCopy(validAlarm);
		input.faults[0].event[prop] = {};
		testCases.push({
		    'name': 'fault: bad "event.' + prop + '"',
		    'objtype': 'alarm',
		    'input': input,
		    'errmsg': new RegExp('property "faults\\[0\\].event.' +
		        prop + '": object value found, but .* required')
		});
	});

	/*
	 * Semantically bad alarm objects
	 */

	input = jsprim.deepCopy(validAlarm);
	input.faults[0].type = 'other';
	testCases.push({
	    'name': 'fault: unsupported "type"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "faults\[0\].type":.*enumeration/
	});

	input = jsprim.deepCopy(validAlarm);
	input.faults[0].event.type = 'other';
	testCases.push({
	    'name': 'fault: unsupported "type"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "faults\[0\].event.type":.*enumeration/
	});

	input = jsprim.deepCopy(validAlarm);
	input.faults[0].event.v = 2;
	testCases.push({
	    'name': 'fault: unsupported version"',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /property "faults\[0\].event.v":.*enumeration/
	});

	input = jsprim.deepCopy(validAlarm);
	input.faults = [];
	testCases.push({
	    'name': 'alarm: open with no faults',
	    'objtype': 'alarm',
	    'input': input,
	    'errmsg': /alarm open with no faults/
	});

	input = jsprim.deepCopy(validAlarm);
	input.closed = true;
	testCases.push({
	    'name': 'alarm: inconsistent close state (1)',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /alarm's "closed" is not consistent with "timeClosed"/
	});

	input = jsprim.deepCopy(validAlarm);
	input.timeClosed = input.timeLastEvent;
	testCases.push({
	    'name': 'alarm: inconsistent close state (2)',
	    'objtype': 'alarm',
	    'input': input,
	    /* JSSTYLED */
	    'errmsg': /alarm's "closed" is not consistent with "timeClosed"/
	});


	/*
	 * Probe group objects
	 */

	validProbeGroup = {
	    'uuid': 'uuid-1',
	    'name': 'honor roller',
	    'user': 'user-uuid-1',
	    'disabled': false,
	    'contacts': [ 'contact1', 'contact2' ]
	};

	testCases.push({
	    'name': 'probe group: basic case',
	    'objtype': 'probegroup',
	    'input': validProbeGroup,
	    'verify': function verifyBasicProbeGroup(pg) {
		assertplus.strictEqual(pg.pg_name, 'honor roller');
		assertplus.strictEqual(pg.pg_user, 'user-uuid-1');
		assertplus.strictEqual(pg.pg_uuid, 'uuid-1');
		assertplus.strictEqual(pg.pg_enabled, true);
		assertplus.deepEqual(
		    pg.pg_contacts, [ 'contact1', 'contact2' ]);
	    }
	});

	input = jsprim.deepCopy(validProbeGroup);
	input.disabled = true;
	delete (input.contacts);
	testCases.push({
	    'name': 'probe group: disabled, no contacts',
	    'objtype': 'probegroup',
	    'input': input,
	    'verify': function verifyDisabledProbeGroup(pg) {
		assertplus.strictEqual(pg.pg_name, 'honor roller');
		assertplus.strictEqual(pg.pg_user, 'user-uuid-1');
		assertplus.strictEqual(pg.pg_uuid, 'uuid-1');
		assertplus.strictEqual(pg.pg_enabled, false);
		assertplus.deepEqual(pg.pg_contacts, []);
	    }
	});

	[ 'uuid', 'name', 'user', 'disabled' ].forEach(
	    function (prop) {
		input = jsprim.deepCopy(validProbeGroup);
		delete (input[prop]);
		testCases.push({
		    'name': 'probe group: missing "' + prop + '"',
		    'objtype': 'probegroup',
		    'input': input,
		    'errmsg': new RegExp('property "' + prop + '":.*missing')
		});

		input = jsprim.deepCopy(validProbeGroup);
		input[prop] = 37;
		testCases.push({
		    'name': 'probe group: bad type for "' + prop + '"',
		    'objtype': 'probegroup',
		    'input': input,
		    'errmsg': new RegExp('property "' + prop +
		        '": number value found, but.* is required')
		});
	    });

	/*
	 * Probe objects
	 */

	validProbe = {
	    'uuid': 'uuid-1',
	    'name': 'probe-1',
	    'type': 'cmd',
	    'config': {},
	    'agent': 'agent-uuid-1',
	    'groupEvents': false,
	    'machine': null,
	    'group': null,
	    'contacts': [ 'contact1', 'contact2' ]
	};

	input = jsprim.deepCopy(validProbe);
	testCases.push({
	    'name': 'probe: basic case (1)',
	    'objtype': 'probe',
	    'input': input,
	    'verify': function verifyBasicProbe(p) {
		assertplus.strictEqual(p.p_uuid, 'uuid-1');
		assertplus.strictEqual(p.p_name, 'probe-1');
		assertplus.strictEqual(p.p_type, 'cmd');
		assertplus.deepEqual(p.p_config, {});
		assertplus.strictEqual(p.p_agent, 'agent-uuid-1');
		assertplus.strictEqual(p.p_groupid, null);
		assertplus.strictEqual(p.p_machine, null);
		assertplus.deepEqual(p.p_contacts, [ 'contact1', 'contact2' ]);
		assertplus.strictEqual(p.p_group_events, false);
	    }
	});

	/*
	 * Alternate valid probe: for the remaining tests, we'll use a probe
	 * representation that more closely matches what we usually see in
	 * practice: "groupEvents" is true; "machine" and "group" are specified;
	 * and "contacts" is not.
	 */
	validProbe.groupEvents = true;
	validProbe.machine = 'machine-uuid-1';
	validProbe.group = 'group-uuid-1';
	delete (validProbe.contacts);

	input = jsprim.deepCopy(validProbe);
	testCases.push({
	    'name': 'probe: basic case (2)',
	    'objtype': 'probe',
	    'input': input,
	    'verify': function verifyAltProbe(p) {
		assertplus.strictEqual(p.p_uuid, 'uuid-1');
		assertplus.strictEqual(p.p_name, 'probe-1');
		assertplus.strictEqual(p.p_type, 'cmd');
		assertplus.deepEqual(p.p_config, {});
		assertplus.strictEqual(p.p_agent, 'agent-uuid-1');
		assertplus.strictEqual(p.p_groupid, 'group-uuid-1');
		assertplus.strictEqual(p.p_machine, 'machine-uuid-1');
		assertplus.deepEqual(p.p_contacts, null);
		assertplus.strictEqual(p.p_group_events, true);
	    }
	});

	/*
	 * Test missing required fields.  Many of the fields that may be "null"
	 * are still required.
	 */
	[ 'type', 'config', 'agent', 'groupEvents', 'machine',
	    'group' ].forEach(function (prop) {
		input = jsprim.deepCopy(validProbe);
		delete (input[prop]);
		testCases.push({
		    'name': 'probe: missing prop "' + prop + '"',
		    'objtype': 'probe',
		    'input': input,
		    'errmsg': new RegExp('property "' + prop + '":.*missing')
		});
	    });

	[ 'uuid', 'name', 'type', 'config', 'agent', 'groupEvents',
	    'machine', 'group', 'contacts' ].forEach(function (prop) {
		input = jsprim.deepCopy(validProbe);
		input[prop] = 37;
		testCases.push({
		    'name': 'probe: bad "' + prop + '"',
		    'objtype': 'probe',
		    'input': input,
		    'errmsg': new RegExp('property "' + prop +
		        '": number.*found.*required')
		});
	    });
}

main();
