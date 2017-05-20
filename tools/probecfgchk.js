#!/usr/bin/env node

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * probecfgchk: validates a given probe configuration file
 */

var cmdutil = require('cmdutil');
var vasync = require('vasync');

var alarm_metadata = require('../lib/alarms/metadata');
var nerrors = 0;

function main()
{
	cmdutil.configure({
	    'synopses': [ 'FILENAME...' ],
	    'usageMessage': 'validates one or more probe template files'
	});

	if (process.argv.length < 3) {
		cmdutil.usage();
	}

	vasync.forEachPipeline({
	    'func': validateOneFile,
	    'inputs': process.argv.slice(2)
	}, function () {
		process.exit(nerrors === 0 ? 0 : 1);
	});
}

function validateOneFile(filename, callback)
{
	var pts;

	pts = new alarm_metadata.MetadataLoader();
	pts.loadFromFile(filename, function onLoaded() {
		var errors;

		errors = pts.errors();
		nerrors += errors.length;

		if (errors.length === 0) {
			console.error('%s okay', filename);
		} else {
			errors.forEach(function (e) { cmdutil.warn(e); });
		}

		callback();
	});
}

main();
