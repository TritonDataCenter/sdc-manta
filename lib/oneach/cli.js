/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/oneach/cli.js: command-line parser for the "manta-oneach" tool.  This
 * exists in a separate file primarily so that it can be automatically tested.
 */

var assertplus = require('assert-plus');
var getopt = require('posix-getopt');
var VError = require('verror').VError;

var oneach = require('./oneach');
var sdc = require('../sdc');


/* Public interface */
exports.mzParseCommandLine = mzParseCommandLine;


/*
 * Default values for command-line arguments.
 */
var mzConcurrency = 10;			/* concurrency for Ur commands */
var mzExecTimeoutDefault = 60 * 1000;	/* milliseconds */
var mzAmqpConnectTimeoutDefault = 5000;	/* milliseconds */
var mzAmqpPortDefault = 5672;		/* standard amqp port */
var mzAmqpLoginDefault = 'guest';	/* suitable for sdc/manta */
var mzAmqpPasswordDefault = 'guest';	/* suitable for sdc/manta */


/*
 * The short option letters for the AMQP options are not documented and not
 * intended to be used.  We similarly don't document one for --dry-run.  -n is
 * customary, but might be confused with sdc-oneachnode's "-n" argument, which
 * is somewhat different than the analogous flag here (which is -S).
 */
var mzOptionStr = [
    'A:(amqp-host)',
    'B:(amqp-password)',
    'C:(amqp-login)',
    'D:(amqp-port)',
    'E:(amqp-timeout)',
    'F(dry-run)',

    'a(all-zones)',
    'c:(concurrency)',
    'd:(dir)',
    'g:(get)',
    'G(global-zones)',
    'p:(put)',
    's:(service)',
    'z:(zonename)',
    'I(immediate)',
    'J(jsonstream)',
    'N(oneline)',
    'S:(compute-node)',
    'T:(exectimeout)',
    'X(clobber)'
].join('');


/*
 * Given "argv" (which is typically `process.argv` with the leading two values
 * sliced off), parse command-line arguments according to the interface defined
 * for the "manta-oneach" command.  On failure, returns an Error object or null
 * (see below).  On success, returns an object with a combination of fields:
 *
 *     o fields used as arguments to the mzCommandExecutor() constructor
 *     o fields used as arguments to the output formatters
 *     o additional fields:
 *
 *         outputBatch   (boolean)
 *         outputMode    (enum)		"text" or "jsonstream"
 *         multilineMode (enum)		"one", "multi", or "auto"
 *
 * out of "argv".  A non-Error, non-null return value represents part of the
 * argument to mzCommandExecutor().
 *
 * There's only one case where "null" is returned, which is when there's a
 * command-line syntax error that has already been emitted to stderr.
 */
function mzParseCommandLine(argv)
{
	var parser, option, args, p, err;
	var multiline = null;

	args = {
	    'amqpHost': null,
	    'amqpPort': mzAmqpPortDefault,
	    'amqpTimeout': mzAmqpConnectTimeoutDefault,
	    'amqpLogin': mzAmqpLoginDefault,
	    'amqpPassword': mzAmqpPasswordDefault,
	    'sdcMantaConfigFile': sdc.sdcMantaConfigPathDefault,

	    'scopeAllZones': false,
	    'scopeComputeNodes': null,
	    'scopeZones': null,
	    'scopeServices': null,
	    'scopeGlobalZones': false,

	    'concurrency': mzConcurrency,
	    'dryRun': false,
	    'streamStatus': process.stderr,

	    'execMode': oneach.MZ_EM_COMMAND,
	    'execTimeout': mzExecTimeoutDefault,
	    'execCommand': null,
	    'execFile': null,
	    'execDirectory': null,
	    'execClobber': null,
	    'bindIp': null,

	    'omitHeader': false,
	    'outputMode': 'text',
	    'outputBatch': true,
	    'multilineMode': 'auto'
	};

	parser = new getopt.BasicParser(mzOptionStr, argv, 0);
	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		/*
		 * The AMQP and dry-run options are given undocumented short
		 * options to satisfy getopt.
		 */
		case 'A':
			args.amqpHost = option.optarg;
			break;

		case 'B':
			args.amqpPassword = option.optarg;
			break;

		case 'C':
			args.amqpLogin = option.optarg;
			break;

		case 'D':
			p = parseInt(option.optarg, 10);
			if (isNaN(p) || p <= 0) {
				return (new VError(
				    'expected positive integer for ' +
				    '--amqp-port, but got: %s', option.optarg));
			}
			args.amqpPort = p;
			break;

		case 'E':
			p = parseInt(option.optarg, 10);
			if (isNaN(p) || p <= 0) {
				return (new VError(
				    'expected positive integer for ' +
				    '--amqp-timeout, but got: %s',
				    option.optarg));
			}
			args.amqpTimeout = p * 1000;
			break;

		case 'F':
			args.dryRun = true;
			break;


		/*
		 * Scoping options
		 */

		case 'a':
			args.scopeAllZones = true;
			break;

		case 's':
			if (args.scopeServices === null) {
				args.scopeServices = [];
			}

			args.scopeServices = appendCommaSeparatedList(
			    args.scopeServices, option.optarg);
			break;

		case 'z':
			if (args.scopeZones === null) {
				args.scopeZones = [];
			}
			args.scopeZones = appendCommaSeparatedList(
			    args.scopeZones, option.optarg);
			break;

		case 'G':
			args.scopeGlobalZones = true;
			break;

		case 'S':
			if (args.scopeComputeNodes === null) {
				args.scopeComputeNodes = [];
			}

			args.scopeComputeNodes = appendCommaSeparatedList(
			    args.scopeComputeNodes, option.optarg);
			break;

		/*
		 * Other options
		 */
		case 'c':
			p = parseInt(option.optarg, 10);
			if (isNaN(p) || p <= 0) {
				return (new VError(
				    'expected positive integer for ' +
				    '-c/--concurrency, but got: %s',
				    option.optarg));
			}
			args.concurrency = p;
			break;

		case 'd':
			args.execDirectory = option.optarg;
			break;

		case 'g':
			if (args.execMode != oneach.MZ_EM_COMMAND) {
				return (new VError('unexpected --get'));
			}
			args.execFile = option.optarg;
			args.execMode = oneach.MZ_EM_SENDTOREMOTE;
			if (args.execClobber === null) {
				args.execClobber = false;
			}
			break;

		case 'p':
			if (args.execMode != oneach.MZ_EM_COMMAND) {
				return (new VError('unexpected --put'));
			}
			args.execFile = option.optarg;
			args.execMode = oneach.MZ_EM_RECEIVEFROMREMOTE;
			break;

		case 'I':
			args.multilineMode = 'multi';
			args.outputBatch = false;
			break;

		case 'J':
			args.outputMode = 'jsonstream';
			break;

		case 'N':
			multiline = 'one';
			break;

		case 'T':
			p = parseInt(option.optarg, 10);
			if (isNaN(p) || p <= 0) {
				return (new VError(
				    'expected positive integer for ' +
				    '-T/--exectimeout, but got: %s',
				    option.optarg));
			}
			args.execTimeout = p * 1000;
			break;

		case 'X':
			args.execClobber = true;
			break;

		default:
			/* error message already emitted by getopt */
			assertplus.equal('?', option.option);
			return (null);
		}
	}


	/*
	 * The --oneline option overrides the implied semantics of --immediate,
	 * regardless of the order in which they were specified.
	 */
	if (multiline == 'one') {
		args.multilineMode = 'one';
	}

	if (args.execMode == oneach.MZ_EM_COMMAND) {
		if (parser.optind() >= argv.length) {
			return (new Error('expected command'));
		}

		if (parser.optind() < argv.length - 1) {
			return (new Error('unexpected arguments'));
		}

		if (args.execDirectory !== null) {
			return (new Error('--dir cannot be used without ' +
			    '--put or --get'));
		}

		args.execCommand = argv[parser.optind()];
	} else {
		if (parser.optind() < argv.length) {
			return (new Error('unexpected arguments'));
		}

		if (args.execDirectory === null) {
			return (new Error(
			    '--dir is required with --put and --get'));
		}
	}

	/*
	 * We've checked the syntax of the command-line by this point.  Now
	 * check the semantics: many combinations of options are not valid.
	 */
	err = oneach.mzValidateScopeParameters(args);
	if (err instanceof Error) {
		return (err);
	}

	return (args);
}

/*
 * Given an array "list" and a string value "strval" that may contain several
 * comma-separated values, split "strval" by commas and append each of the
 * resulting substrings to "list".  Empty substrings are ignored.  This is used
 * to allow users to specify options like:
 *
 *     -z ZONENAME1,ZONENAME2,ZONENAME3
 *
 * as equivalent to:
 *
 *     -z ZONENAME1 -z ZONENAME2 -z ZONENAME3
 */
function appendCommaSeparatedList(list, strval)
{
	return (list.concat(strval.split(',').filter(
	    function (s) { return (s.length > 0); })));
}
