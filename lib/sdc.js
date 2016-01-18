/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/sdc.js: library functions for working with SDC
 */

var assertplus = require('assert-plus');
var fs = require('fs');
var path = require('path');
var VError = require('verror').VError;

/* Public interface */
exports.sdcMantaConfigPathDefault =
    path.join(__dirname, '..', 'etc', 'config.json');
exports.sdcReadAmqpConfig = sdcReadAmqpConfig;

/*
 * Given a path to the sdc-manta configuration file "configpath", read the file
 * and invoke "callback" with an object mapping configuration variables to
 * values for the AMQP part of the configuration.  Failures to read the file
 * will result in an operational error passed to the callback.
 */
function sdcReadAmqpConfig(configpath, callback)
{
	assertplus.string(configpath, 'configpath');
	assertplus.func(callback, 'callback');

	fs.readFile(configpath, function (err, data) {
		var parsed;

		if (err) {
			callback(new VError(err, 'read "%s"', configpath));
			return;
		}

		if (!err) {
			try {
				parsed = JSON.parse(data.toString('utf8'));
			} catch (ex) {
				err = new VError(ex, 'parse "%s"', configpath);
			}
		}

		if (!err &&
		    !parsed.hasOwnProperty('amqp') ||
		    typeof (parsed['amqp']['host']) != 'string') {
			err = new VError('expected amqp.host in "%s"',
			    configpath);
		}

		if (err) {
			callback(err);
		} else {
			callback(null, parsed['amqp']);
		}
	});
}
