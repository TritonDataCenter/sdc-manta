/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.alarms.js: tests interfaces for iterating and updating Amon alarms.
 * Like the config tests, these use a mock Amon server.  These tests only
 * exercise the code in lib/alarms/alarms.js that fetch, represent, and modify
 * alarms.  These interfaces don't use any probe group or probe information, nor
 * local metadata.
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var vasync = require('vasync');
var VError = require('verror');

var alarms = require('../../lib/alarms');
var mock_amon = require('./mock_amon');

var account = mock_amon.account;
var alarmsById = {};
var timestamp = Date.parse('2017-05-03T00:00:00Z');
var tsiso = new Date(timestamp).toISOString();
var mockAmon;

function main()
{
	var log;

	log = new bunyan({
	    'name': 'tst.alarms.js',
	    'level': process.env['LOG_LEVEL'] || 'fatal',
	    'stream': process.stderr
	});

	vasync.waterfall([
		function init(callback) {
			mock_amon.createMockAmon(log, function (mock) {
				mockAmon = mock;
				callback();
			});
		},

		function loadFail(callback) {
			mockAmon.config = { 'alarms': { 'all': 'error' } };
			alarms.amonLoadAlarmsForState({
			    'account': account,
			    'amon': mockAmon.client,
			    'state': 'all'
			}, function (err, alarmlist) {
				assertplus.ok(err);
				assertplus.ok(err instanceof Error);
				assertplus.ok(
				    /listing open alarms: injected error/.test(
				    err.message));
				assertplus.ok(!alarmlist);
				callback();
			});
		},

		function loadAlarmsOpen(callback) {
			mockAmon.config = {
			    'alarms': { 'open': [
				alarmsById['35'],
				alarmsById['40'],
				/* invalid alarm having no id */
				{},
				alarmsById['45'],
				alarmsById['50'],
				alarmsById['55']
			    ] }
			};

			alarms.amonLoadAlarmsForState({
			    'account': account,
			    'amon': mockAmon.client,
			    'state': 'open'
			}, function (err, alarmset) {
				var warnings, list, a, f;

				/*
				 * As long as "alarmset" is present, then "err"
				 * only indicates warning-level issues.
				 */
				assertplus.ok(err);
				assertplus.ok(alarmset);

				/*
				 * Check the warnings.
				 */
				warnings = [];
				VError.errorForEach(err, function (e) {
					warnings.push(e.message);
				});
				warnings.sort();

				assertplus.deepEqual(warnings, [
				    'bad alarm from server: alarm 45: ' +
				    'alarm\'s "closed" is not consistent ' +
				    'with "timeClosed"',

				    'bad alarm from server: property "id": ' +
				    'is missing and it is required'
				]);

				/*
				 * Check the list of alarms itself.
				 */
				list = [];
				alarmset.eachAlarm(function (id, aa) {
					list.push({ 'id': id, 'alarm': aa });
				});

				list = list.sort(function (a1, a2) {
					return (a1.id - a2.id);
				});

				list.forEach(function (l) {
					assertplus.ok(l.alarm ==
					    alarmset.alarmForId(l.id));
				});

				/*
				 * Examine the first alarm in detail.
				 */
				a = list[0].alarm;
				assertplus.equal(a.a_id, 35);
				assertplus.equal(a.a_user, account);
				assertplus.equal(a.a_groupid, 'group-35');
				assertplus.strictEqual(a.a_closed, false);
				assertplus.strictEqual(a.a_suppressed, false);
				assertplus.equal(a.a_time_opened.toISOString(),
				    tsiso);
				assertplus.strictEqual(a.a_time_closed, null);
				assertplus.equal(a.a_time_last.toISOString(),
				    tsiso);
				assertplus.equal(a.a_nevents, 3);
				assertplus.equal(a.a_faults.length, 1);

				f = a.a_faults[0];
				assertplus.equal(f.aflt_alarm, a);
				assertplus.equal(f.aflt_probeid,
				    'probe-uuid-35');
				assertplus.strictEqual(f.aflt_clear, false);
				assertplus.equal(f.aflt_uuid, 'event-uuid-35');
				assertplus.equal(f.aflt_machine, 'machine-35');
				assertplus.equal(f.aflt_agent,
				    'agent-uuid-35');
				assertplus.equal(f.aflt_agent_alias,
				    'agent-name-35');
				assertplus.equal(f.aflt_time.toISOString(),
				    tsiso);
				assertplus.equal(f.aflt_summary, 'boom (35)');

				/*
				 * The second one is largely the same, but has
				 * no group.
				 */
				a = list[1].alarm;
				assertplus.equal(a.a_id, 40);
				assertplus.strictEqual(a.a_groupid, null);

				callback();
			});
		},

		function loadAlarmsById(callback) {
			mockAmon.config = {
			    'alarms': { 'by_id': {
				'35': alarmsById['35'],
				'40': alarmsById['40'],
				'42': 'error',
				'45': alarmsById['45']
			    } }
			};

			/*
			 * This request exercises several cases, including
			 * non-integer alarms, duplicate alarm requests, invalid
			 * alarms from the server, requests that we've
			 * configured the mock amon to fail with a 500, and
			 * alarms that don't exist.
			 */
			alarms.amonLoadAlarmsForIds({
			    'account': account,
			    'amon': mockAmon.client,
			    'alarmIds': [
			        '35', '45', '37', 'bogus', '40', '35', '42' ],
			    'concurrency': 3
			}, function (err, alarmset) {
				var warnings, list;

				/*
				 * As before, there should be an error, but also
				 * some alarms
				 */
				assertplus.ok(err);
				assertplus.ok(alarmset);

				warnings = [];
				VError.errorForEach(err, function (e) {
					warnings.push(e.message);
				});
				warnings.sort();

				assertplus.deepEqual(warnings, [
				    'alarm "bogus": invalid number: "bogus"',
				    'bad alarm from server: alarm 45: ' +
					'alarm\'s "closed" is not consistent ' +
					'with "timeClosed"',
				    'fetch alarm "37": alarm not found',
				    'fetch alarm "42": injected error'
				]);

				list = [];
				alarmset.eachAlarm(function (id) {
					list.push(id);
				});
				list = list.sort();
				assertplus.deepEqual(list, [ 35, 40 ]);

				callback();
			});
		},

		/*
		 * Exercise alarm "close".
		 */
		function closeAlarms(callback) {
			mockAmon.config = {
			    'alarms_close': [],
			    'alarms': { 'by_id': {
				'35': alarmsById['35'],
				'40': alarmsById['40'],
				'42': 'error'
			    } }
			};

			/*
			 * As above, this request exercises several cases,
			 * including non-integer alarms, duplicate alarm
			 * requests, requests that we've configured the mock
			 * amon to fail with a 500, and alarms that don't exist.
			 */
			alarms.amonCloseAlarms({
			    'account': account,
			    'amon': mockAmon.client,
			    'alarmIds': [
			        '35', '37', 'bogus', '40', '42' ],
			    'concurrency': 3
			}, function (err) {
				var errors;

				assertplus.ok(err);
				errors = [];
				VError.errorForEach(err, function (e) {
					errors.push(e.message);
				});
				errors.sort();

				assertplus.deepEqual(errors, [
				    'alarm "bogus": invalid number: "bogus"',
				    'close alarm "37": alarm not found',
				    'close alarm "42": injected error'
				]);

				assertplus.deepEqual(
				    mockAmon.config.alarms_close.sort(),
				    [ '35', '40' ]);
				callback();
			});
		},

		/*
		 * Exercise alarm notification disable.
		 */
		function disableNotifications(callback) {
			mockAmon.config = {
			    'alarms_suppress': [],
			    'alarms': { 'by_id': {
				'35': alarmsById['35'],
				'40': alarmsById['40'],
				'42': 'error'
			    } }
			};

			/*
			 * As above, this request exercises several cases,
			 * including non-integer alarms, duplicate alarm
			 * requests, requests that we've configured the mock
			 * amon to fail with a 500, and alarms that don't exist.
			 */
			alarms.amonUpdateAlarmsNotification({
			    'account': account,
			    'amonRaw': mockAmon.clientRaw,
			    'suppressed': true,
			    'alarmIds': [
			        '35', '37', 'bogus', '40', '42' ],
			    'concurrency': 3
			}, function (err) {
				var errors;

				assertplus.ok(err);
				errors = [];
				VError.errorForEach(err, function (e) {
					errors.push(e.message);
				});
				errors.sort();

				assertplus.deepEqual(errors, [
				    'alarm "bogus": invalid number: "bogus"',
				    'disable notifications for alarm 37: ' +
				        'alarm not found',
				    'disable notifications for alarm 42: ' +
				        'injected error'
				]);

				assertplus.deepEqual(
				    mockAmon.config.alarms_suppress.sort(),
				    [ '35', '40' ]);
				callback();
			});
		},

		/*
		 * Exercise alarm notification enable.
		 */
		function enableNotifications(callback) {
			mockAmon.config = {
			    'alarms_unsuppress': [],
			    'alarms': { 'by_id': {
				'35': alarmsById['35'],
				'40': alarmsById['40'],
				'42': 'error'
			    } }
			};

			/*
			 * As above, this request exercises several cases,
			 * including non-integer alarms, duplicate alarm
			 * requests, requests that we've configured the mock
			 * amon to fail with a 500, and alarms that don't exist.
			 */
			alarms.amonUpdateAlarmsNotification({
			    'account': account,
			    'amonRaw': mockAmon.clientRaw,
			    'suppressed': false,
			    'alarmIds': [
			        '35', '37', 'bogus', '40', '42' ],
			    'concurrency': 3
			}, function (err) {
				var errors;

				assertplus.ok(err);
				errors = [];
				VError.errorForEach(err, function (e) {
					errors.push(e.message);
				});
				errors.sort();

				assertplus.deepEqual(errors, [
				    'alarm "bogus": invalid number: "bogus"',
				    'enable notifications for alarm 37: ' +
				        'alarm not found',
				    'enable notifications for alarm 42: ' +
				        'injected error'
				]);

				assertplus.deepEqual(
				    mockAmon.config.alarms_unsuppress.sort(),
				    [ '35', '40' ]);
				callback();
			});
		}
	], function (err) {
		if (err) {
			throw (err);
		}

		mockAmon.server.close();
		console.log('%s okay', __filename);
	});
}

/* normal, complete alarm */
alarmsById['35'] = {
    'id': 35,
    'user': account,
    'probeGroup': 'group-35',
    'closed': false,
    'suppressed': false,
    'timeOpened': timestamp,
    'timeClosed': null,
    'timeLastEvent': timestamp,
    'numEvents': 3,
    'faults': [ {
	    'type': 'probe',
	    'probe': 'probe-uuid-35',
	    'event': {
	    'v': 1,
	    'type': 'probe',
	    'clear': false,
	    'machine': 'machine-35',
	    'uuid': 'event-uuid-35',
	    'agent': 'agent-uuid-35',
	    'agentAlias': 'agent-name-35',
	    'time': timestamp,
	    'data': {
		'message': 'boom (35)'
	    }
	}
    } ]
};

/* normal alarm having no group */
alarmsById['40'] = {
    'id': 40,
    'user': account,
    'closed': false,
    'suppressed': false,
    'timeOpened': timestamp,
    'timeClosed': null,
    'timeLastEvent': timestamp,
    'numEvents': 3,
    'faults': [ {
	'type': 'probe',
	'probe': 'probe-uuid-40',
	'event': {
	    'v': 1,
	    'type': 'probe',
	    'clear': false,
	    'machine': 'machine-40',
	    'uuid': 'event-uuid-40',
	    'agent': 'agent-uuid-40',
	    'agentAlias': 'agent-name-40',
	    'time': timestamp,
	    'data': {
		'message': 'boom (40)'
	    }
	}
    } ]
};

/* invalid alarm: closed/timeClosed */
alarmsById['45'] = {
    'id': 45,
    'user': account,
    'closed': true,
    'suppressed': false,
    'timeOpened': timestamp,
    'timeClosed': null,
    'timeLastEvent': timestamp,
    'numEvents': 3,
    'faults': [ {
	'type': 'probe',
	'probe': 'probe-uuid-nonexistent',
	'event': {
	    'v': 1,
	    'type': 'probe',
	    'clear': false,
	    'machine': 'machine-45',
	    'uuid': 'event-uuid-45',
	    'agent': 'agent-uuid-45',
	    'agentAlias': 'agent-name-45',
	    'time': timestamp,
	    'data': {
		'message': 'boom (45)'
	    }
	}
    } ]
};

/* alarm with non-existent probe group */
alarmsById['50'] = {
    'id': 50,
    'user': account,
    'probeGroup': 'unknown-group',
    'closed': false,
    'suppressed': false,
    'timeOpened': timestamp,
    'timeClosed': null,
    'timeLastEvent': timestamp,
    'numEvents': 3,
    'faults': [ {
	'type': 'probe',
	'probe': 'probe-uuid-nonexistent',
	'event': {
	    'v': 1,
	    'type': 'probe',
	    'clear': false,
	    'machine': 'machine-50',
	    'uuid': 'event-uuid-50',
	    'agent': 'agent-uuid-50',
	    'agentAlias': 'agent-name-50',
	    'time': timestamp,
	    'data': {
		'message': 'boom (50)'
	    }
	}
    } ]
};

/* alarm with non-existent probe */
alarmsById['55'] = {
    'id': 55,
    'user': account,
    'closed': false,
    'suppressed': false,
    'timeOpened': timestamp,
    'timeClosed': null,
    'timeLastEvent': timestamp,
    'numEvents': 3,
    'faults': [ {
	'type': 'probe',
	'probe': 'probe-uuid-nonexistent',
	    'event': {
	    'v': 1,
	    'type': 'probe',
	    'clear': false,
	    'machine': 'machine-55',
	    'uuid': 'event-uuid-55',
	    'agent': 'agent-uuid-55',
	    'agentAlias': 'agent-name-55',
	    'time': timestamp,
	    'data': {
		'message': 'boom (55)'
	    }
	}
    } ]
};

main();
