/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.metadata_basic.js: tests probe template metadata subsystem
 */

var assertplus = require('assert-plus');
var extsprintf = require('extsprintf');
var jsprim = require('jsprim');
var VError = require('verror');

var sprintf = extsprintf.sprintf;

var alarm_metadata = require('../../lib/alarms/metadata');
var services = require('../../lib/services');

var testCases = [];
var sampleEvent = 'upset.manta.test_event';
var sampleLegacyName = 'my sample probe';
var sampleScope = { 'service': 'madtom' };
var sampleChecks = [ { 'type': 'cmd', 'config': { 'test': 'prop' } } ];
var sampleKa = {
    'title': 'sample title',
    'description': 'sample description',
    'severity': 'sample severity',
    'response': 'sample response',
    'impact': 'sample impact',
    'action': 'sample action'
};

var sampleTemplate = {
    'event': sampleEvent,
    'legacyName': sampleLegacyName,
    'scope': sampleScope,
    'checks': sampleChecks,
    'ka': sampleKa
};

function main()
{
	generateTestCases();
	testCases.forEach(runTestCase);
	console.error('%s okay', __filename);
}

function generateTestCases()
{
	/*
	 * Test a case that loads 0 templates.
	 */
	testCases.push({
	    'name': 'basic, valid input with 0 templates',
	    'input': JSON.stringify([]),
	    'verify': function verifyEmpty(md) {
		md.eachEvent(function (eventName) {
			throw (new VError('empty metadata found an event: "%s"',
			    eventName));
		});

		md.eachTemplate(function (tpl) {
			throw (new VError('empty metadata found a template'));
		});
	    }
	});

	/*
	 * Test basic results of the success path, including all of the
	 * interfaces that allow consumers to interact with the parsed metadata.
	 */
	testCases.push({
	    'name': 'basic, valid input (exhaustive)',
	    'input': JSON.stringify([ sampleTemplate ]),
	    'verify': function verifyValid(md) {
		var list, ka, pt, pgname, evtname, parsed;

		/*
		 * Test eachEvent().  This interface is exported at-large to
		 * consumers in sdc-manta.
		 */
		list = [];
		md.eachEvent(function (eventName) { list.push(eventName); });
		assertplus.deepEqual(list, [ sampleEvent ]);

		/*
		 * Test eventKa().  This interface is exported at-large to
		 * consumers in sdc-manta.
		 */
		ka = md.eventKa(sampleEvent + '.junk');
		assertplus.strictEqual(ka, null);
		ka = md.eventKa(sampleEvent);
		assertplus.notStrictEqual(ka, null);
		assertplus.equal(ka.ka_title, sampleKa.title);
		assertplus.equal(ka.ka_description, sampleKa.description);
		assertplus.equal(ka.ka_severity, sampleKa.severity);
		assertplus.equal(ka.ka_response, sampleKa.response);
		assertplus.equal(ka.ka_impact, sampleKa.impact);
		assertplus.equal(ka.ka_action, sampleKa.action);

		/*
		 * Test eachTemplate() and the ProbeTemplate class fields.  This
		 * interface is exported only within the alarms configuration
		 * subsystem of sdc-manta.
		 */
		list = [];
		md.eachTemplate(function (tpl) { list.push(tpl); });
		assertplus.equal(list.length, 1);
		pt = list[0];
		assertplus.equal(pt.pt_event, sampleEvent);
		assertplus.string(pt.pt_origin_label);
		assertplus.equal(pt.pt_scope.ptsc_service, sampleScope.service);
		assertplus.strictEqual(pt.pt_scope.ptsc_global, false);
		assertplus.strictEqual(pt.pt_scope.ptsc_check_from, null);
		assertplus.deepEqual(pt.pt_ka, ka);
		assertplus.arrayOfObject(pt.pt_checks);
		assertplus.equal(pt.pt_checks.length, 1);
		assertplus.equal(pt.pt_checks[0].ptc_type,
		    sampleChecks[0].type);
		assertplus.deepEqual(pt.pt_checks[0].ptc_config,
		    sampleChecks[0].config);
		assertplus.deepEqual(pt.pt_aliases, []);

		/*
		 * Tests constructing and parsing probe group names.  These
		 * interfaces are exported only within the alarms configuration
		 * subsystem of sdc-manta.
		 */
		pgname = alarm_metadata.probeGroupNameForTemplate(
		    pt, sampleEvent);
		assertplus.equal(pgname, sampleEvent + ';v=1');
		/* a valid probe group name */
		parsed = alarm_metadata.testingParseProbeGroupName(pgname);
		assertplus.strictEqual(parsed.error, null);
		assertplus.strictEqual(parsed.isLegacy, false);
		assertplus.strictEqual(parsed.isOther, false);
		assertplus.strictEqual(parsed.eventName, sampleEvent);

		/* a legacy probe group name */
		parsed = alarm_metadata.testingParseProbeGroupName('ops-alert');
		assertplus.strictEqual(parsed.error, null);
		assertplus.strictEqual(parsed.isLegacy, true);
		assertplus.strictEqual(parsed.isOther, false);
		assertplus.strictEqual(parsed.eventName, null);

		/* an operator-created probe group name */
		parsed = alarm_metadata.testingParseProbeGroupName('mygroup');
		assertplus.strictEqual(parsed.error, null);
		assertplus.strictEqual(parsed.isLegacy, false);
		assertplus.strictEqual(parsed.isOther, true);
		assertplus.strictEqual(parsed.eventName, null);

		/* a probe group name that fails to parse */
		parsed = alarm_metadata.testingParseProbeGroupName(
		    'mygroup;v=2');
		assertplus.object(parsed.error, 'error');
		assertplus.ok(parsed.error instanceof Error);
		/* JSSTYLED */
		assertplus.ok(/unrecognized version "2" in probe group/.test(
		    parsed.error.message));
		assertplus.strictEqual(parsed.isLegacy, false);
		assertplus.strictEqual(parsed.isOther, false);
		assertplus.strictEqual(parsed.eventName, null);

		/*
		 * Test probeGroupEventName() and probeGroupIsRemovable().
		 * These interfaces are exported only within the alarms
		 * configuration subsystem of sdc-manta.
		 */
		assertplus.strictEqual(null, md.probeGroupEventName('foobar'));
		assertplus.strictEqual(false,
		    md.probeGroupIsRemovable('foobar'));
		/* a legacy probe group name */
		assertplus.strictEqual(null,
		    md.probeGroupEventName('ops-alert'));
		assertplus.strictEqual(true,
		    md.probeGroupIsRemovable('ops-alert'));
		/* an operator-created probe group name */
		assertplus.strictEqual(null,
		    md.probeGroupEventName('my-custom-group'));
		assertplus.strictEqual(false,
		    md.probeGroupIsRemovable('my-custom-group'));
		assertplus.strictEqual(null,
		    md.probeGroupEventName('my-custom-group; with semicolon'));
		assertplus.strictEqual(false,
		    md.probeGroupIsRemovable('my-custom-group; with semi'));
		/* a malformed probe group name */
		assertplus.strictEqual(null,
		    md.probeGroupEventName(sampleEvent + ';v=2'));
		assertplus.strictEqual(false,
		    md.probeGroupIsRemovable(sampleEvent + ';v=2'));
		/* a configured probe group name */
		evtname = md.probeGroupEventName(pgname);
		assertplus.strictEqual(evtname, sampleEvent);
		assertplus.strictEqual(false, md.probeGroupIsRemovable(pgname));
	    }
	});

	/* Test case that creates two simple templates. */
	testCases.push({
	    'name': 'basic, valid input with 2 templates',
	    'input': JSON.stringify([ {
	        'event': sampleEvent + '.1',
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa
	    }, {
	        'event': sampleEvent + '.2',
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'verify': function verifyTwo(md) {
		var events, pts, pgname, evtname;

		events = [];
		md.eachEvent(function (eventName) {
			events.push(eventName);
		});
		events = events.sort();
		assertplus.deepEqual(events, [
		    sampleEvent + '.1', sampleEvent + '.2' ]);

		pts = [];
		md.eachTemplate(function (pt) {
			pts.push(pt);
		});
		pts.sort(function (pt1, pt2) {
			assertplus.string(pt1.pt_event);
			assertplus.string(pt2.pt_event);
			return (pt1.pt_event.localeCompare(pt2.pt_event));
		});

		assertplus.equal(pts[0].pt_event, events[0]);
		assertplus.equal(pts[1].pt_event, events[1]);
		pgname = alarm_metadata.probeGroupNameForTemplate(
		    pts[0], events[0]);
		evtname = md.probeGroupEventName(pgname);
		assertplus.equal(evtname, events[0]);
		assertplus.ok(!md.probeGroupIsRemovable(pgname));
	    }
	});

	/*
	 * Test cases with invalid input.
	 */

	testCases.push({
	    'name': 'invalid YAML',
	    'input': '{',
	    /* JSSTYLED */
	    'errmsg': /parse "input": unexpected end/
	});

	testCases.push({
	    'name':  'schema mismatch: not an array',
	    'input': JSON.stringify({}),
	    /* JSSTYLED */
	    'errmsg': /parse "input":.*object.*found.*array.*required/
	});

	/*
	 * For each required field, produce a test case that attempts to parse
	 * otherwise valid input that's missing that field.
	 */
	[ 'event', 'scope', 'checks', 'ka' ].forEach(function (field) {
		var input = jsprim.deepCopy(sampleTemplate);
		delete (input[field]);

		testCases.push({
		    'name': sprintf(
		        'schema mismatch: missing required field: "%s"', field),
		    'input': JSON.stringify([ input ]),
		    'errmsg': new RegExp('^parse "input": property ' +
		        '"0.' + field + '":.*missing')
		});
	});

	/* Bad type for "event" */
	testCases.push({
	    'name': 'schema mismatch: bad "event"',
	    'input': JSON.stringify([ {
		'event': {},
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.event": ' +
	        'object.*found,.*string.*required')
	});

	/* Bad type for "legacyName" */
	testCases.push({
	    'name': 'schema mismatch: bad "legacyName"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'legacyName': {},
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': 'busted'
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.legacyName": ' +
	        'object.*found,.*string.*required')
	});

	/* Bad type for "scope" */
	testCases.push({
	    'name': 'schema mismatch: bad "scope"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': 'bad scope',
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.scope": ' +
	        'string.*found,.*object.*required')
	});

	/* Bad type for "checks" */
	testCases.push({
	    'name': 'schema mismatch: bad "checks"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': {},
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.checks": ' +
	        'object.*found,.*array.*required')
	});

	/* Bad type for "ka" */
	testCases.push({
	    'name': 'schema mismatch: bad "ka"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': 'busted'
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.ka": ' +
	        'string.*found,.*object.*required')
	});

	/* Extra top-level properties */
	testCases.push({
	    'name': 'schema mismatch: extra top-level property',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa,
		'something': 'anotherValue'
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.something": ' +
	        'unsupported property')
	});

	/* "scope": bad service name */
	testCases.push({
	    'name': 'schema mismatch: bad "scope.service"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': { 'service': 'bogus' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.scope.service":' +
	        ' does not have a value in the enumeration')
	});

	/* "scope": extra properties */
	testCases.push({
	    'name': 'schema mismatch: bad "scope" (extra properties)',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': { 'service': 'madtom', 'extraProp': 17 },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property ' +
	        '"0.scope.extraprop": unsupported property')
	});

	/* "scope": bad "global" */
	testCases.push({
	    'name': 'schema mismatch: bad "scope.global"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': { 'service': 'madtom', 'global': 'false' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.scope.global": ' +
	        'string.*found,.*boolean.*required')
	});

	/* "scope": bad "checkFrom" */
	testCases.push({
	    'name': 'schema mismatch: bad "scope.checkFrom"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': { 'service': 'madtom', 'checkFrom': 'each' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property ' +
	        '"0.scope.checkFrom": does not have a value in the enumeration')
	});

	/* "checks": none */
	testCases.push({
	    'name': 'schema mismatch: no checks',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': [],
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property "0.checks": ' +
	        '.*minimum of 1.*')
	});

	/* "checks": bad type */
	testCases.push({
	    'name': 'schema mismatch: check has bad type',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': [ {
		    'type': 'unsupported'
		} ],
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^parse "input": property ' +
	        '"0.checks\\[0\\].type": does not have a value ' +
		'in the enumeration')
	});

	/* "ka": string field has bad type */
	testCases.push({
	    'name': 'schema mismatch: ka has non-string field',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': {
		    'title': {}
		}
	    } ]),
	    'errmsg': new RegExp('^parse "input": property ' +
	        '"0.ka.title": object.*found.*string.*required')
	});

	/* "ka": string field has trailing newline */
	testCases.push({
	    'name': 'bad input: ka has non-string field',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': {
		    'title': 'my title',
		    'description': 'my description',
		    'response': 'my response\n',
		    'action': 'an action\nokay\n\nokay here too',
		    'severity': 'major',
		    'impact': 'none'
		}
	    } ]),
	    'errmsg': new RegExp('^input: probe 1: field ka.response: ' +
	        'ends with trailing newline')
	});

	/* duplicate event name */
	testCases.push({
	    'name': 'bad input: duplicate event name',
	    'input': JSON.stringify([ sampleTemplate, sampleTemplate ]),
	    'errmsg': new RegExp('^input: probe 2: re-uses event name ' +
	        '"upset.manta.test_event" previously used in template ' +
		'"input: probe 1"$')
	});

	/* event name with invalid characters */
	testCases.push({
	    'name': 'bad input: unsupported event name',
	    'input': JSON.stringify([ {
	        'event': sampleEvent + '-1',
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^input: probe 1: event name contains ' +
	        'unsupported characters')
	});

	/* variables not allowed when scope is not "each" */
	testCases.push({
	    'name': 'bad input: attempted use of variables without "each"',
	    'input': JSON.stringify([ {
	        'event': sampleEvent + '.$foo',
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^input: probe 1: event name contains ' +
	        'unsupported characters')
	});

	/* event name doesn't start with "upset.manta. */
	testCases.push({
	    'name': 'bad input: unsupported event name',
	    'input': JSON.stringify([ {
	        'event': 'upset.manta_stuff.another_event',
		'scope': sampleScope,
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^input: probe 1: field "event": must begin ' +
	        'with "upset.manta."$')
	});

	/*
	 * Test cases making use of the "each" scope.  These generate multiple
	 * aliases and they require the use of the $service variable.
	 */

	/* missing using '$service' */
	testCases.push({
	    'name': 'bad input: use of "each" without "$service"',
	    'input': JSON.stringify([ {
		'event': sampleEvent,
		'scope': { 'service': 'each' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^template "input: probe 1": templates ' +
	        'with scope "each" must use "\\$service" in event name to ' +
		'ensure uniqueness$')
	});

	/* use of unsupported variables */
	testCases.push({
	    'name': 'bad input: use of "each" with unsupported variable',
	    'input': JSON.stringify([ {
		'event': sampleEvent + '.$service.$other',
		'scope': { 'service': 'each' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^template "input: probe 1": unknown ' +
	        'variable "\\$other" in event name$')
	});

	/* unsupported characters after expansion */
	testCases.push({
	    'name': 'bad input: use of "each" without "$service"',
	    'input': JSON.stringify([ {
		'event': sampleEvent + '.$service.other-junk',
		'scope': { 'service': 'each' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'errmsg': new RegExp('^input: probe 1: expanded ' +
	        'event name contains unsupported characters')
	});

	/* valid input using "each" */
	testCases.push({
	    'name': 'valid input using "each" scope',
	    'input': JSON.stringify([ {
		'event': sampleEvent + '.$service',
		'scope': { 'service': 'each' },
		'checks': sampleChecks,
		'ka': sampleKa
	    } ]),
	    'verify': function verifyValidEach(md) {
		var expectedServices, expectedEvents, list, ka, pt;
		var pgname, evtname;

		/*
		 * See the other valid test case above for the structure of this
		 * verifier.
		 *
		 * We should see events for each service that supports probes.
		 * This excludes "marlin".
		 */
		expectedServices = services.mSvcNamesProbes.slice(0).sort();
		expectedEvents = expectedServices.map(function (svcname) {
			return (sampleEvent + '.' + svcname.replace(/-/g, '_'));
		});

		list = [];
		md.eachEvent(function (eventName) { list.push(eventName); });
		assertplus.deepEqual(list.sort(), expectedEvents);

		ka = md.eventKa('upset.manta.test_event.authcache');
		assertplus.equal(ka.ka_title, sampleKa.title);

		list = [];
		md.eachTemplate(function (tpl) { list.push(tpl); });
		assertplus.equal(list.length, 1);
		pt = list[0];
		assertplus.equal(pt.pt_event, sampleEvent + '.$service');
		assertplus.deepEqual(pt.pt_ka, ka);
		assertplus.equal(pt.pt_scope.ptsc_service, 'each');

		/*
		 * Check the aliases.
		 */
		pt.pt_aliases.slice(0).sort(function (a1, a2) {
			assertplus.string(a1.pta_service);
			assertplus.string(a2.pta_service);
			return (a1.pta_service.localeCompare(a2.pta_service));
		}).forEach(function (a, i) {
			assertplus.equal(a.pta_event, expectedEvents[i]);
			assertplus.equal(a.pta_service, expectedServices[i]);
		});

		/*
		 * Check the event name <-> probe group translations.
		 */
		pgname = alarm_metadata.probeGroupNameForTemplate(pt,
		    expectedEvents[0]);
		evtname = md.probeGroupEventName(pgname);
		assertplus.equal(evtname, expectedEvents[0]);

		pgname = alarm_metadata.probeGroupNameForTemplate(pt,
		    expectedEvents[1]);
		evtname = md.probeGroupEventName(pgname);
		assertplus.equal(evtname, expectedEvents[1]);
		assertplus.strictEqual(false, md.probeGroupIsRemovable(pgname));
	    }
	});
}

/*
 * Each test case must have:
 *
 *     name (string)		human-readable name for the test case
 *
 *     input (string)		string input to parse as a probe template
 *
 * and ONE of the following:
 *
 *     errmsg (regexp)		Parsing the input must produce a single error
 *     				matching this regular expression.
 *
 *     verify (function)	Parsing the input must produce no errors.
 *     				The resulting metadata will be passed to this
 *     				function for additional verification.  The
 *     				function should throw an exception for failures.
 */
function runTestCase(testcase)
{
	var mdl, errors;

	assertplus.object(testcase, 'testcase');
	assertplus.string(testcase.name, 'testcase.name');
	assertplus.string(testcase.input, 'testcase.input');

	console.error('test case: %s', testcase.name);
	mdl = new alarm_metadata.MetadataLoader();
	mdl.loadFromString(testcase.input, 'input');
	errors = mdl.errors();
	assertplus.ok(errors.length < 2,
	    'test suite did not expect multiple parse errors');

	if (testcase.hasOwnProperty('errmsg')) {
		assertplus.ok(testcase.errmsg instanceof RegExp,
		    'expected error message must be a regular expression');

		if (errors.length === 0) {
			throw (new VError(
			    'expected error ("%s"), but found none',
			    testcase.errmsg.source));
		}

		if (!testcase.errmsg.test(errors[0].message)) {
			throw (new VError('error message mismatch: found ' +
			    '"%s", expected "%s"', errors[0].message,
			    testcase.errmsg.source));
		}

		console.error('found matching error message');
	} else {
		assertplus.func(testcase.verify, 'testcase.verify');

		if (errors.length > 0) {
			throw (new VError('expected no error, but found: "%s"',
			    errors[0].message));
		}

		testcase.verify(mdl.mdl_amoncfg);
	}

	console.error('');
}

main();
