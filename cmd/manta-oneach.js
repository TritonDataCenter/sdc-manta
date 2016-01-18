#!/usr/bin/env node
/* vim: set ft=javascript: */
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
    'SCOPE_ARGUMENTS [OPTIONS] OPERATION_ARGUMENTS'
];

var mzUsageMessage = [
    '',
    'Execute a shell command on all Manta zones or a subset of zones using ',
    'filters based on zonename, service name, or compute node.',
    '',
    'SCOPE ARGUMENTS',
    '',
    '    -a | --all-zones                     all non-marlin, non-global zones',
    '    -S | --compute-node HOSTNAME|UUID... zones on named compute node',
    '    -s | --service SERVICE...            zones of SAPI service SERVICE',
    '    -z | --zonename ZONENAME...          specified zones only',
    '    -G | --global-zones                  operate on global zones of ',
    '                                         whichever zones would otherwise',
    '                                         have been operated on',
    '',
    'OPERATION ARGUMENTS',
    '',
    '    Command execution: OPERATION_ARGUMENTS consists of a single string ',
    '    argument containing an arbitrary bash script to execute in each zone.',
    '',
    '    File transfer: Either the -p/--put or -g/--get option must be ',
    '    specified, plus the -d/--dir option.  The -X/--clobber option may ',
    '    also be used.',
    '',
    '    -g | --get FILE                      causes the remote target to ',
    '                                         fetch the local file FILE ',
    '                                         into the remote directory ',
    '                                         specified with --dir.',
    '',
    '    -p | --put FILE                      causes the remote target to ',
    '                                         upload the remote file FILE ',
    '                                         into the local directory ',
    '                                         specified with --dir.',
    '',
    '    -d | --dir DIR                       see --get and --put',
    '',
    '    -X | --clobber                       allow --get to overwrite an',
    '                                         existing local file.',
    '',
    'OTHER OPTIONS',
    '',
    '    -c | --concurrency N                 number of operations to allow ',
    '                                         outstanding at any given time',
    '',
    '    -I | --immediate                     emit results as they arrive, ',
    '                                         rather than sorted at the end',
    '',
    '    -J | --jsonstream                    emit newline-separated JSON',
    '                                         output, similar to ',
    '                                         sdc-oneachnode(1), but with ',
    '                                         additional "zonename" and ',
    '                                         "service" properties (unless ',
    '                                         --global-zones was specified).',
    '                                         Implies --immediate.',
    '',
    '    --dry-run                            report what would be executed ',
    '                                         without actually running it',
    '',
    '    -N | --oneline                       report only the last line of ',
    '                                         output from each command',
    '',
    '    -T | --exectimeout SECONDS           command execution timeout',
    '                                         (same as for sdc-oneachnode(1))',
    '                                         default: 60 seconds',
    '',
    '    --amqp-host HOST                     AMQP connection parameters',
    '    --amqp-port TCP_PORT                 default: auto-configured',
    '    --amqp-login LOGIN',
    '    --amqp-password PASSWORD',
    '    --amqp-timeout SECONDS               default: 5 seconds',
    '',
    'You must specify either -a/--all-zones or at least one of the other',
    'scope arguments.  -a/--all-zones cannot be combined with the other',
    'arguments.  The other arguments can be combined, and the result is to',
    'operate on zones matching all of the specified criteria.  For example:',
    '',
    '    manta-oneach --compute-node MS08214 --service storage COMMAND',
    '',
    'executes COMMAND on on all "storage" zones on compute node MS08214.',
    '',
    'You can use --global-zones to operate on the global zones hosting ',
    'the zones that would otherwise have been matched.  For example:',
    '',
    '    manta-oneach --global-zones --service=webapi COMMAND',
    '',
    'executes COMMAND in the global zones of all compute nodes containing at ',
    'least one "webapi" zone.'
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
