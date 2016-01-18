/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * common.js: common code for various tests
 */

var sdc = require('../lib/sdc');

/* Public interface */
exports.defaultCommandExecutorArgs = defaultCommandExecutorArgs;

function defaultCommandExecutorArgs()
{
	/*
	 * These are the same defaults used in mzParseCommandLine().  They're
	 * repeated here rather than referencing that copy directly to make sure
	 * that changes to that code require that these tests be updated
	 * appropriately.
	 *
	 * The value here is also not necessarily exactly the same object as the
	 * default value in mzParseCommandLine().  For example, we elide
	 * streamStatus because it needs to be checked differently than the
	 * other properties.
	 */
	return ({
	    'amqpHost': null,
	    'amqpPort': 5672,
	    'amqpTimeout': 5000,
	    'amqpLogin': 'guest',
	    'amqpPassword': 'guest',
	    'sdcMantaConfigFile': sdc.sdcMantaConfigPathDefault,

	    'scopeAllZones': false,
	    'scopeComputeNodes': null,
	    'scopeZones': null,
	    'scopeServices': null,
	    'scopeGlobalZones': false,

	    'concurrency': 10,
	    'dryRun': false,

	    'execMode': null,
	    'execTimeout': 60000,
	    'execCommand': null,
	    'execFile': null,
	    'execDirectory': null,
	    'execClobber': null,
	    'bindIp': null,

	    'omitHeader': false,
	    'outputMode': 'text',
	    'outputBatch': true,
	    'multilineMode': 'auto'
	});
}
