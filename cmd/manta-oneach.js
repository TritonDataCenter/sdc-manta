#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * manta-oneach: execute a shell command on all Manta zones, or a subset of
 * zones using filters based on zonename, service name, or compute node.
 * See usage message for details.
 */

var assert = require('assert');
var bunyan = require('bunyan');
var cmdutil = require('cmdutil');

var oneach = require('../lib/oneach/oneach');
var oneach_cli = require('../lib/oneach/cli');

var mzSynopses = [
    'OPTIONS SCOPE_ARGUMENTS COMMAND',
    'OPTIONS SCOPE_ARGUMENTS -d|--dir DIRECTORY -g|--get FILE',
    'OPTIONS SCOPE_ARGUMENTS -d|--dir DIRECTORY -p|--put FILE'
];

var mzUsageMessage = [
    '',
    'Execute a shell command on all Manta zones or a subset of zones using ',
    'filters based on zonename, service name, or compute node.',
    '',
    '    -a, --all-zones                        select all zones',
    '    -S, --compute-node HOSTNAME|UUID...    select zones on specified CN',
    '    -s, --service SERVICE...               select zones for SERVICE',
    '    -z, --zonename ZONENAME...             select specific zones',
    '    -G, --global-zones                     select global zones',
    '',
    '    -g, --get FILE                         transfer local to remote',
    '    -p, --put FILE                         transfer remote to local',
    '    -d, --dir DIR                          varies with -g or -p',
    '    -X, --clobber                          clobber existing files with -g',
    '',
    '    -c, --concurrency N                    max concurrency',
    '    -I, --immediate                        print results as they arrive',
    '    -J, --jsonstream                       json-streaming output',
    '    -N, --oneline                          one line of output per result',
    '    -T, --exectimeout SECONDS              execution timeout',
    '    --dry-run                              dry run mode',
    '',
    '    --amqp-host HOST',
    '    --amqp-port TCP_PORT',
    '    --amqp-login LOGIN',
    '    --amqp-password PASSWORD',
    '    --amqp-timeout SECONDS'
].join('\n');

function main()
{
	var args, exec, next;

	cmdutil.exitOnEpipe();
	cmdutil.configure({
	    'synopses': mzSynopses,
	    'usageMessage': mzUsageMessage
	});

	args = oneach_cli.mzParseCommandLine(process.argv.slice(2));
	if (args instanceof Error) {
		cmdutil.usage(args);
	}

	if (args === null) {
		/* error message emitted by getopt */
		cmdutil.usage();
	}

	/*
	 * By default, we hide virtually all bunyan log messages.  It would be
	 * better to log these to a local file, but we don't have a great
	 * solution for this.  ("/var/log" isn't necessarily writable, and
	 * "/var/tmp", isn't appropriate.  Plus, we want to append to the log
	 * rather than to replace whatever's there.  We also want to make sure
	 * it gets flushed on operational errors.)  We don't want to clutter the
	 * user's terminal, even when things go wrong, since we should be
	 * reporting actionable operational errors through the usual mechanisms.
	 * Users can enable logging by setting LOG_LEVEL.
	 */
	args.log = new bunyan({
	    'name': 'manta-oneach',
	    'level': process.env['LOG_LEVEL'] || 'fatal'
	});

	exec = new oneach.mzCommandExecutor(args);
	if (args.outputMode == 'text') {
		next = new oneach.mzResultToText({
		    'omitHeader': args.omitHeader,
		    'outputBatch': args.outputBatch,
		    'multilineMode': args.multilineMode
		});
	} else {
		assert.equal(args.outputMode, 'jsonstream');
		next = new oneach.mzResultToJson();
	}

	exec.pipe(next);
	next.pipe(process.stdout);

	exec.on('error', function (err) {
		cmdutil.fail(err);
	});

	process.stdout.on('finish', function () {
		if (exec.nexecerrors() > 0) {
			process.exit(1);
		}
	});
}

main();
