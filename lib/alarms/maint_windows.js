/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * lib/alarms/maint_windows.js: manage maintenance windows
 */

var assertplus = require('assert-plus');
var extsprintf = require('extsprintf');
var jsprim = require('jsprim');
var vasync = require('vasync');
var VError = require('verror');
var sprintf = extsprintf.sprintf;

var amon_objects = require('./amon_objects');

exports.amonLoadMaintWindows = amonLoadMaintWindows;
exports.amonCreateMaintWindow = amonCreateMaintWindows;
exports.amonDeleteMaintWindows = amonDeleteMaintWindows;

/*
 * Load the list of Amon maintenance windows from the Amon master API, creating
 * an AmonMaintWindow object for each valid window found on the server.  Named
 * arguments include:
 *
 *    account (string)	the Triton account for which to fetch windows
 *
 *    amonRaw (object)	a restify client created for the Amon master API.
 *    			Note that this is not an sdc-clients Amon client.
 *
 * The callback is invoked as callback(err, windows), where "windows" (if
 * present) is a list of AmonMaintWindow objects.  If "windows" is falsey, then
 * the call completely failed, and "err" indicates why.  It's possible for "err"
 * and "windows" to both be present, which indicates that we successfully
 * retrieved a list of windows, but some of them could not be interpreted
 * correctly.  "err" describes the problems in interpreting those windows.
 */
function amonLoadMaintWindows(args, callback) {
    var client, account, uripath;

    assertplus.object(args, 'args');
    assertplus.string(args.account, 'args.account');
    assertplus.object(args.amonRaw, 'args.amonRaw');

    client = args.amonRaw;
    account = args.account;
    uripath = sprintf('/pub/%s/maintenances', encodeURIComponent(account));
    client.get(uripath, function(err, req, res, rawWindows) {
        var warnings, windows;

        if (err) {
            err = new VError(err, 'amon: get "%s"', uripath);
            callback(err);
            return;
        }

        warnings = [];
        windows = [];
        rawWindows.forEach(function(rawWindow) {
            var maintwin = amon_objects.loadMaintWindow(rawWindow);
            if (!(maintwin instanceof Error) && maintwin.win_user !== account) {
                maintwin = new VError(
                    'window %d: account does not match expected',
                    maintwin.win_id
                );
            }

            if (maintwin instanceof Error) {
                warnings.push(maintwin);
            } else {
                windows.push(maintwin);
            }
        });

        callback(VError.errorFromList(warnings), windows);
    });
}

/*
 * Create a single maintenance window.  Named arguments include:
 *
 *    account (string)  the Triton account in which to create the window
 *
 *    amonRaw (object)  a restify client created for the Amon master API
 *                      Note that this is not an sdc-clients Amon client.
 *
 *    windef (object)   defines the window itself
 *
 *        start (Date)     start time of the window
 *
 *        end (Date)       end time of the window
 *
 *        notes (string)   notes for the window
 *
 *        all (boolean)    indicates that this is not scoped to machines,
 *                         probes, or probe groups
 *
 *        machines         limit scope to specified array of machine uuids
 *                         (strings)
 *
 *        probes           limit scope to specified array of probe uuids
 *                         (strings)
 *
 *        probeGroups       limit scope to specified array of probe group uuids
 *                          (strings)
 *
 * Note that only one of "all", "machines", "probes", or "probe groups" may be
 * specified.  See the Amon Master API documentation for details.
 */
function amonCreateMaintWindows(args, callback) {
    var client, account, uripath, winparams;

    assertplus.object(args, 'args');
    assertplus.string(args.account, 'args.account');
    assertplus.object(args.amonRaw, 'args.amonRaw');
    assertplus.object(args.windef, 'args.windef');
    assertplus.object(args.windef.start, 'args.windef.start');
    assertplus.ok(args.windef.start instanceof Date);
    assertplus.object(args.windef.end, 'args.windef.end');
    assertplus.ok(args.windef.end instanceof Date);
    assertplus.optionalString(args.windef.notes, 'args.windef.notes');
    assertplus.optionalBool(args.windef.all, 'args.windef.all');
    assertplus.optionalArrayOfString(args.windef.probes, 'args.windef.probes');
    assertplus.optionalArrayOfString(
        args.windef.probeGroups,
        'args.windef.probeGroups'
    );
    assertplus.optionalArrayOfString(
        args.windef.machines,
        'args.windef.machines'
    );

    winparams = {};
    winparams.start = args.windef.start.toISOString();
    winparams.end = args.windef.end.toISOString();
    if (typeof args.windef.notes === 'string') {
        winparams.notes = args.windef.notes;
    }

    if (args.windef.probes) {
        assertplus.ok(!args.windef.probeGroups);
        assertplus.ok(!args.windef.machines);
        winparams.probes = args.windef.probes;
    } else if (args.windef.probeGroups) {
        assertplus.ok(!args.windef.machines);
        winparams.probeGroups = args.windef.probeGroups;
    } else if (args.windef.machines) {
        winparams.machines = args.windef.machines;
    } else {
        winparams.all = true;
    }

    client = args.amonRaw;
    account = args.account;
    uripath = sprintf('/pub/%s/maintenances', encodeURIComponent(account));
    client.post(uripath, winparams, function(err, req, res, obj) {
        if (!err) {
            obj = amon_objects.loadMaintWindow(obj);
            if (obj instanceof Error) {
                err = new VError(
                    obj,
                    'window created, but could not process response'
                );
            } else if (obj.win_user !== account) {
                err = new VError(
                    obj,
                    'window created, but returned with a different account'
                );
            }
        }

        if (err) {
            err = new VError(err, 'amon: post "%s"', uripath);
            callback(err);
        } else {
            callback(err, obj);
        }
    });
}

/*
 * Deletes the specified list of windows.
 *
 * Named arguments:
 *
 *    account (string)	the Triton account for which to fetch windows
 *
 *    amonRaw (object)	a restify client created for the Amon master API.
 *    			Note that this is not an sdc-clients Amon client.
 *
 *    winIds (array)    array of strings identifying windows to delete
 *
 *    concurrency (int) number of operations to attempt in parallel
 *
 * Note that the window ids are validated, and invalid windows are operational
 * errors (not programmer errors).
 */
function amonDeleteMaintWindows(args, callback) {
    var client, account, errors, queue;

    assertplus.object(args, 'args');
    assertplus.string(args.account, 'args.account');
    assertplus.object(args.amonRaw, 'args.amonRaw');
    assertplus.arrayOfString(args.winIds, 'args.winIds');
    assertplus.number(args.concurrency, 'args.concurrency');

    client = args.amonRaw;
    account = args.account;
    errors = [];
    queue = vasync.queuev({
        concurrency: args.concurrency,
        worker: function deleteWindow(winid, qcallback) {
            var num, uripath;

            num = jsprim.parseInteger(winid);
            if (typeof num === 'number' && num < 1) {
                num = VError('not a positive integer');
            }

            if (num instanceof Error) {
                errors.push(new VError(num, 'window "%s"', winid));
                qcallback();
                return;
            }

            assertplus.equal(typeof num, 'number');
            uripath = sprintf(
                '/pub/%s/maintenances/%d',
                encodeURIComponent(account),
                num
            );
            client.del(uripath, function(err) {
                if (err) {
                    err = new VError(err, 'amon: delete "%s"', uripath);
                    errors.push(err);
                }

                qcallback();
            });
        }
    });

    args.winIds.forEach(function(w) {
        queue.push(w);
    });
    queue.on('end', function() {
        callback(VError.errorFromList(errors));
    });

    queue.close();
}
