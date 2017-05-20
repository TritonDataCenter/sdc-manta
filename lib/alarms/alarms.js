/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/alarms/alarms.js: facilities for working with amon alarms.
 */

var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var vasync = require('vasync');

var sprintf = require('extsprintf').sprintf;
var VError = require('verror');

var amon_objects = require('./amon_objects');

/* Exported interface */
exports.amonLoadAlarmsForState = amonLoadAlarmsForState;
exports.amonLoadAlarmsForIds = amonLoadAlarmsForIds;
exports.amonCloseAlarms = amonCloseAlarms;
exports.amonUpdateAlarmsNotification = amonUpdateAlarmsNotification;


/*
 * Load information about Amon alarms in the specified state.
 *
 * Named arguments:
 *
 *     account         Triton account uuid whose open alarms to load
 *
 *     amon            Amon client (from sdc-clients)
 *
 *     state           one of "open", "closed", "all", or "recent"
 *
 * The callback is invoked as callback(err, alarmset).  "alarmset" is a
 * MantaAlarmSet that is provided if any alarms could be loaded.  It may not be
 * provided if no alarms could be loaded.  "err" describes any issues loading
 * alarms.  You can have any combination of "err" and "alarmset".
 */
function amonLoadAlarmsForState(args, callback)
{
	var amon, account, rv, options;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.amon, 'args.amon');
	assertplus.string(args.state, 'args.state');

	amon = args.amon;
	account = args.account;
	rv = new MantaAlarmSet();
	options = { 'state': args.state };

	amon.listAlarms(account, options, function (err, rawalarms) {
		var errors;

		if (err) {
			err = new VError(err, 'listing open alarms');
			callback(err);
			return;
		}

		errors = [];
		rawalarms.forEach(function (rawalarm) {
			var alarm;

			alarm = amon_objects.loadAlarmObject(rawalarm);
			if (alarm instanceof Error) {
				errors.push(new VError(alarm,
				    'bad alarm from server'));
				return;
			}

			if (rv.hasAlarmWithId(alarm.a_id)) {
				errors.push(new VError('server reported ' +
				    'more than one alarm with id %d',
				    alarm.a_id));
				return;
			}

			rv.addAlarm(alarm);
		});

		err = VError.errorFromList(errors);
		rv.finalize();
		callback(err, rv);
	});
}

/*
 * Iterate the specified "alarmIds" and invoke "func" for each one.
 *
 * External to this file, we avoid assuming that alarm ids are positive
 * integers.  That's an amon-ism.  But the Amon client library does assume that,
 * so here's where we have to validate it.  This function also manages a queue
 * of the requested concurrency.
 */
function amonAlarmForEach(args, callback)
{
	var errors, queue, func;

	assertplus.object(args, 'args');
	assertplus.arrayOfString(args.alarmIds, 'args.alarmIds');
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.func(args.func, 'args.func');

	errors = [];
	func = args.func;
	queue = vasync.queuev({
	    'concurrency': args.concurrency,
	    'worker': function iterAlarm(alarmid, qcallback) {
		var num;

		num = jsprim.parseInteger(alarmid);
		if (typeof (num) == 'number' && num < 1) {
			num = VError('not a positive integer');
		}

		if (num instanceof Error) {
			errors.push(new VError(num, 'alarm "%s"', alarmid));
			qcallback();
			return;
		}

		func(num, function onFuncDone(err) {
			if (err) {
				errors.push(err);
			}

			qcallback();
		});
	    }
	});

	args.alarmIds.forEach(function (a) { queue.push(a); });
	queue.on('end', function () {
		callback(VError.errorFromList(errors));
	});

	queue.close();
}

/*
 * Closes the specified open alarms.
 *
 * Named arguments:
 *
 *     account         Triton account uuid whose open alarms to load
 *
 *     amon            Amon client (from sdc-clients)
 *
 *     alarmIds	       array of alarm ids to close
 *
 *     concurrency     maximum request concurrency
 *
 * This is an array-based interface in order to better support parallelizing
 * operations.  This could also expose an object-mode stream interface.
 */
function amonCloseAlarms(args, callback)
{
	var account, amon;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.amon, 'args.amon');
	assertplus.arrayOfString(args.alarmIds, 'args.alarmIds');
	assertplus.number(args.concurrency, 'args.concurrency');

	account = args.account;
	amon = args.amon;
	amonAlarmForEach({
	    'alarmIds': args.alarmIds,
	    'concurrency': args.concurrency,
	    'func': function amonCloseOne(alarmid, subcallback) {
		assertplus.number(alarmid);
		amon.closeAlarm(account, alarmid, function onAmonClose(err) {
			if (err) {
				err = new VError(err,
				    'close alarm "%d"', alarmid);
			}

			subcallback(err);
		});
	    }
	}, callback);
}


/*
 * Updates the "suppressed" property on the specified alarms.
 *
 * Named arguments:
 *
 *     account         Triton account uuid whose open alarms to load
 *
 *     amonRaw         a restify JSON client for the AMON master API.
 *                     This is different from most other consumers, which use an
 *                     actual Amon client.
 *
 *     alarmIds        array of alarm ids to close
 *
 *     concurrency     maximum request concurrency
 *
 *     suppressed      new value for the "suppressed" property
 *
 * This is an array-based interface in order to better support parallelizing
 * operations.  This could also expose an object-mode stream interface.
 */
function amonUpdateAlarmsNotification(args, callback)
{
	var account, amon, suppressed;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.amonRaw, 'args.amonRaw');
	assertplus.arrayOfString(args.alarmIds, 'args.alarmIds');
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.bool(args.suppressed, 'args.suppressed');

	account = args.account;
	amon = args.amonRaw;
	suppressed = args.suppressed;

	amonAlarmForEach({
	    'alarmIds': args.alarmIds,
	    'concurrency': args.concurrency,
	    'func': function amonUpdateOne(alarmid, subcallback) {
		/*
		 * Unfortunately, sdc-client's Amon client does not support this
		 * operation, so we need to hit the API directly.
		 *
		 * The server also doesn't recognize POST parameters specified
		 * in the body, so we have to put them into the query string.
		 */
		var action, resource;
		action = suppressed ? 'suppress' : 'unsuppress';
		resource = sprintf('/pub/%s/alarms/%d?action=%s',
		    encodeURIComponent(account), alarmid, action);
		amon.post(resource, function (err) {
			if (err) {
				err = new VError(err,
				    '%s notifications for alarm %d',
				    suppressed ? 'disable' : 'enable',
				    alarmid);
			}
			subcallback(err);
		});
	    }
	}, callback);
}

/*
 * Fetches details about the specified alarms.  Named arguments:
 *
 *     account         Triton account uuid whose open alarms to load
 *
 *     amon            Amon client (from sdc-clients)
 *
 *     alarmIds	       array of alarm ids to close
 *
 *     concurrency     maximum request concurrency
 *
 * The callback is invoked as callback(err, alarmset).  "alarmset" is a
 * MantaAlarmSet that is always provided and contains any alarms that were
 * successfully loaded.  "err" describes any errors loading alarms, and you can
 * have it whether or not "alarmset" has any alarms.
 */
function amonLoadAlarmsForIds(args, callback)
{
	var account, amon, fetching, rv;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.amon, 'args.amon');
	assertplus.arrayOfString(args.alarmIds, 'args.alarmIds');
	assertplus.number(args.concurrency, 'args.concurrency');

	account = args.account;
	amon = args.amon;
	fetching = {};
	rv = new MantaAlarmSet();
	amonAlarmForEach({
	    'alarmIds': args.alarmIds,
	    'concurrency': args.concurrency,
	    'func': function amonLoadOne(alarmid, subcallback) {
		/*
		 * Ignore duplicates.
		 */
		if (fetching[alarmid]) {
			setImmediate(subcallback);
			return;
		}

		fetching[alarmid] = true;
		amon.getAlarm(account, alarmid, function (err, rawalarm) {
			var alarm;

			if (err) {
				subcallback(new VError(
				    err, 'fetch alarm "%d"', alarmid));
				return;
			}

			alarm = amon_objects.loadAlarmObject(rawalarm);
			if (alarm instanceof Error) {
				subcallback(new VError(alarm,
				    'bad alarm from server'));
				return;
			}

			/*
			 * We checked for duplicates before we made the request.
			 */
			rv.addAlarm(alarm);
			subcallback();
		});
	    }
	}, function (err) {
		rv.finalize();
		callback(err, rv);
	});
}

/*
 * Represents a set of open amon alarms.
 */
function MantaAlarmSet()
{
	/* list of open alarms */
	this.mas_alarms = [];

	/* alarms indexed by id */
	this.mas_alarms_byid = {};

	/*
	 * The set goes through two phases: the first phase is where consumers
	 * (within this file) insert alarms, and the second phase is where
	 * consumers (in other subsystems) iterate the alarms.  finalize() must
	 * be called by the first consumer to change phases.  This is currently
	 * used just to ensure that we sort the alarms exactly once, but it also
	 * ensures a clean separation of the two parts of this interface.  (We
	 * could instead create a write-only MantaAlarmSetBuilder and make this
	 * read-only.)
	 */
	this.mas_finalized = false;
}

MantaAlarmSet.prototype.hasAlarmWithId = function (alarmId)
{
	assertplus.number(alarmId, 'alarmId');
	return (this.mas_alarms_byid.hasOwnProperty(alarmId));
};

MantaAlarmSet.prototype.addAlarm = function (alarm)
{
	assertplus.object(alarm, 'alarm');
	assertplus.number(alarm.a_id, 'alarm id');
	assertplus.ok(!this.mas_finalized, 'alarm set is already finalized');
	assertplus.ok(!this.mas_alarms_byid.hasOwnProperty(alarm.a_id));
	this.mas_alarms.push(alarm);
	this.mas_alarms_byid[alarm.a_id] = alarm;
};

MantaAlarmSet.prototype.finalize = function ()
{
	assertplus.ok(!this.mas_finalized, 'alarm set already finalized');
	this.mas_finalized = true;
	this.mas_alarms = this.mas_alarms.sort(function (a1, a2) {
		assertplus.number(a1.a_id);
		assertplus.number(a2.a_id);
		return (a1.a_id - a2.a_id);
	});
};

MantaAlarmSet.prototype.eachAlarm = function (func)
{
	assertplus.ok(this.mas_finalized,
	    'cannot iterate until set is finalized');
	this.mas_alarms.forEach(function (aa) {
		func(aa.a_id, aa);
	});
};

MantaAlarmSet.prototype.alarmForId = function (id)
{
	assertplus.ok(this.mas_finalized,
	    'cannot iterate until set is finalized');
	return (this.mas_alarms_byid.hasOwnProperty(id) ?
	    this.mas_alarms_byid[id] : null);
};
