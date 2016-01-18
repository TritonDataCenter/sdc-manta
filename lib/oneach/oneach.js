/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/oneach/oneach.js: library interface to the "manta-oneach" functionality.
 *
 * The mzCommandExecutor class below is used to manage the execution of commands
 * or file transfers on remote Manta components.  This class implements the Node
 * "Readable" interface (as an object-mode stream) so that the result objects
 * can be piped into one of the output formatters.
 *
 * Two output formatters are provided to ingest the output of an
 * mzCommandExecutor and print results to a stream (usually stdout).  There's a
 * pretty-printing formatter for interactive use and a raw formatter for
 * programmatic consumers.
 *
 * The primary consumer for all this is the "manta-oneach" command, which is a
 * thin wrapper around these functions (plus the command-line parser in
 * lib/oneach/cli.js).
 */

var assertplus = require('assert-plus');
var extsprintf = require('extsprintf');
var forkexec = require('forkexec');
var jsprim = require('jsprim');
var path = require('path');
var stream = require('stream');
var urclient = require('urclient');
var util = require('util');
var vasync = require('vasync');
var zoneutil = require('zonename');

var sdc = require('../sdc');
var madm = require('../adm');

var sprintf = extsprintf.sprintf;
var fprintf = extsprintf.fprintf;
var VError = require('verror').VError;


/* Public interface */
exports.mzCommandExecutor = mzCommandExecutor;
exports.mzResultToText = mzResultToText;
exports.mzResultToJson = mzResultToJson;
exports.mzValidateScopeParameters = mzValidateScopeParameters;


/* Valid values for the "execMode" argument to mzCommandExecutor. */
exports.MZ_EM_COMMAND = 'command';
exports.MZ_EM_SENDTOREMOTE = 'sendToRemote';
exports.MZ_EM_RECEIVEFROMREMOTE = 'receiveFromRemote';

var MZ_EM_COMMAND = exports.MZ_EM_COMMAND;
var MZ_EM_SENDTOREMOTE = exports.MZ_EM_SENDTOREMOTE;
var MZ_EM_RECEIVEFROMREMOTE = exports.MZ_EM_RECEIVEFROMREMOTE;

/*
 * This special value is used when marshalling bash scripts.  See usage for
 * details.
 */
var mzScriptEofMarker = '288dd530';


/*
 * Manages the execution of a single shell command or file transfer on a subset
 * of Manta components.
 *
 *
 * ARGUMENTS
 *
 * Arguments are specified as named properties of the "args" object.  Optional
 * arguments that are left unspecified should be "null" rather than "undefined"
 * or just missing.
 *
 * Arguments related to AMQP configuration:
 *
 *     amqpTimeout        number     AMQP connect timeout
 *
 *     amqpHost           [string]   hostname or IP address of AMQP server
 *     amqpPort           number     TCP port of AMQP server
 *     amqpLogin          string     AMQP authentication login
 *     amqpPassword       string     AMQP authentication password
 *     sdcMantaConfigFile [string]   Path to sdc-manta config file (usually
 *				     use sdcMantaConfigPathDefault in ./sdc.js.)
 *
 *     At least one of sdcMantaConfigFile and amqpHost must be specified.  If
 *     both are specified, the specified amqpHost is used and the config file
 *     path not used.  The other AMQP parameters must always be specified.
 *
 * Arguments related to selecting the scope of operation:
 *
 *     The terminology here matches how people tend to describe these
 *     components, but it's a little confusing on the surface.  There are
 *     basically two scopes of interest:
 *
 *     (1) "Zones": implicitly, the non-global zones that operate most Manta
 *         services, including webapi, jobsupervisor, moray, postgres, and
 *         several others.  This does not include "marlin" zones (also called
 *         "compute" zones).  Operators work with these zones when they want to
 *         inspect or modify most Manta components.
 *
 *     (2) "Global zones": the global zones represent the broadest scope on each
 *         physical server.  Operators work with global zones when they want to
 *         inspect or modify the operating system itself or the Marlin agent
 *         (which is the only Manta component that runs in the global zone).
 *
 *     This tool does not support operating on "marlin" zones.  Besides
 *     generally being unnecessary, this tool's underlying mechanism is not
 *     compatible with the way these zones are managed.  (Running processes
 *     inside these zones can delay or hold up zone shutdown.  Besides that, the
 *     file transfer mechanism used here implicitly trusts the contents of the
 *     zone's filesystem, so it's not secure if untrusted users have access to
 *     the target zone.)
 *
 *     Scopes are specified with several properties:
 *
 *     scopeAllZones	 boolean	Specifies a scope of all "zones"
 *     					(not "global zones").
 *
 *     scopeZones	 [string array]	Specifies a list of non-global zones
 *     					that will be part of the scope.
 *
 *     scopeServices	 [string array]	Specifies a list of Manta SAPI service
 *     					names whose zones should be part of the
 *     					scope.
 *
 *     scopeComputeNodes [string array] Specifies a list of compute nodes
 *     					whose non-global zones should be part of
 *     					the scope.
 *
 *     scopeGlobalZones  boolean	If true, then the scope represents the
 *     					_global_ zones for the zones that would
 *     					otherwise have been used.
 *
 *     This combination allows users to select zones based on the service
 *     they're part of (e.g., "webapi"), their zonename, or the compute node
 *     that they're running on.  It also allows operators to select global zones
 *     matching the same filters (e.g., to operate on global zones of
 *     "loadbalancer" nodes).
 *
 *     For safety, there is no default behavior.  Either "scopeAllZones" or some
 *     combination of zones, services, or compute nodes must be requested (and
 *     not both).
 *
 *     If a combination of zones, services, or compute nodes is specified, then
 *     all of these are applied as filters on the set of all zones.  The result
 *     is the intersection of these sets.
 *
 *     "scopeGlobalZones" is logically applied last, and changes the scope to
 *     match the global zones for whatever components have been specified.
 *
 *
 * Execution-related arguments include:
 *
 *     execMode         enum            one of MZ_EM_COMMAND,
 *     					MZ_EM_SENDTOREMOTE, or
 *     					MZ_EM_RECEIVEFROMREMOTE, indicating
 *     					which operation to execute.
 *
 *     		MZ_EM_COMMAND denotes a bash script specified by 'execCommand'.
 *
 *     		MZ_EM_SENDTOREMOTE denotes a file transfer from 'execFile' on
 *     		the local system to 'execDirectory' on the remote system.
 *
 *     		MZ_EM_RECEIVEFROMREMOTE denotes a file transfer from 'execFile'
 *     		on the remote system to 'execDirectory' on the local system.
 *
 *     execTimeout	number		millisecond timeout for the execution
 *     					of the remote command.  For file
 *     					transfers, this timeout applies to the
 *     					time until the file transfer starts.
 *
 *     execCommand      [string]	shell command to execute.  This script
 *     					is executed as the body of a shell
 *     					script invoked with "bash".  It may
 *     					contain shell redirections, expansions,
 *     					and other special shell characters.
 *     					This is required only if 'execMode' is
 *     					MZ_EM_COMMAND.
 *
 *     execDirectory,   [string]        Used for file transfers.  See
 *     execFile				"execMode".
 *
 *     execClobber      [boolean]       For MZ_EM_SENDTOREMOTE, indicates
 *     					whether the remote file should be
 *     					clobbered if it already exists.
 *
 *     bindIp		[string]	When execMode is MZ_EM_SENDTOREMOTE or
 *     					MZ_EM_RECEIVEFROMREMOTE, this is a local
 *     					IP address on the same network as an IP
 *     					address on the target systems.  This
 *     					would typically be an IP address on the
 *     					SDC "admin" network.  That's because
 *     					"manta" zones have IPs on the "admin"
 *     					network, and the headnode (where this
 *     					library itself is typically used) may
 *     					_only_ have an IP on the "admin"
 *     					network.
 *
 *     					If unspecified, this tool will attempt
 *     					to select an appropriate IP address.
 *     					The current implementation will only
 *     					work when this tool is being run from an
 *     					SDC-managed zone or global zone on the
 *     					"admin" network.
 *
 * Other arguments include:
 *
 *     concurrency	number		how many Ur commands may be outstanding
 *     					at one time
 *
 *     dryRun		boolean		indicates whether we'll actually
 *     					run the command or just report what
 *     					we would do
 *
 *     streamStatus	stream		stream for text status reports
 *     					(e.g., process.stderr)
 *
 *     log		bunyan log	destination for log messages
 *
 *
 *
 * RESULTS
 *
 * This class is an object-mode ReadableStream that does NOT support flow
 * control.  The following events may be emitted (plus any others specified by
 * ReadableStream):
 *
 *     'error'          emitted when an operational error has been encountered
 *                      before execution of the requested command, such as a
 *                      failure to list Manta components or determining that a
 *                      given Manta service name was invalid.  No events will be
 *                      emitted after 'error'.  Per convention, the only
 *                      argument is the Error object itself.
 *
 *                      Failures of an individual command execution (including
 *                      timeouts, exiting with non-zero status, or being
 *                      terminated by a signal) will not show up as an 'error'.
 *                      These will be reported with the corresponding 'data'
 *                      object.
 *
 *     'data'           emitted when a result is available from a command
 *                      invocation or file transfer.  The argument is an object
 *                      with the following properties, which are intended to be
 *                      compatible with the output of sdc-oneachnode(1) and
 *                      node-urclient:
 *
 *              uuid            string   the server_uuid where the command was
 *                                       executed
 *
 *              hostname        string   the hostname of the compute node where
 *                                       the command was executed
 *
 *              zonename        [string] the zonename in which the command was
 *                                       executed.  This will be present only if
 *                                       scopeGlobalZones was false.
 *
 *              service         [string] the SAPI service name for zone
 *                                       "zonename".  This will be present only
 *                                       if "zonename" is present.
 *
 *              result          [object] Ur result, which contains properties
 *                                       "exit_status", "stdout", and "stderr".
 *
 *              error           [object] Error, which contains at least
 *                                       properties "name" and "message".
 *
 *                      Exactly one of "error" or "result" will be present.
 *                      The presence of "error" indicates that either we
 *                      failed to execute the command at all or the status was
 *                      indeterminate because we failed to receive a response
 *                      before "execTimeout" milliseconds had elapsed.  "result"
 *                      will be present if the command was executed and we were
 *                      able to determine within the timeout that the command
 *                      either exited normally (possibly with a non-zero status
 *                      code) or abnormally (i.e., was terminated by a signal).
 *                      Note that you need to check "result" to determine
 *                      whether the command exited non-zero or was killed.
 *
 * Callers will likely want to pipe this stream into one of the output
 * formatters provided below.
 */
function mzCommandExecutor(args)
{
	var self = this;
	var err;

	assertplus.object(args, 'args');

	/*
	 * AMQP parameters.
	 */
	if (args.sdcMantaConfigFile !== null) {
		assertplus.string(args.sdcMantaConfigFile,
		    'args.sdcMantaConfigFile');
		assertplus.optionalString(args.amqpHost, 'args.amqpHost');
	} else {
		assertplus.string(args.amqpHost, 'args.amqpHost');
	}

	assertplus.string(args.amqpLogin, 'args.amqpLogin');
	assertplus.string(args.amqpPassword, 'args.amqpPassword');
	assertplus.number(args.amqpPort, 'args.amqpPort');
	assertplus.number(args.amqpTimeout, 'args.amqpTimeout');

	this.ce_amqp_host = args.amqpHost;
	this.ce_amqp_login = args.amqpLogin;
	this.ce_amqp_password = args.amqpPassword;
	this.ce_amqp_port = args.amqpPort;
	this.ce_amqp_timeout = args.amqpTimeout;
	this.ce_sdc_config_path = args.sdcMantaConfigFile;

	/*
	 * Scope parameters
	 */
	err = mzValidateScopeParameters(args);
	assertplus.ok(err === null, err ? err.message : null);
	this.ce_scope_all_zones = args.scopeAllZones;
	this.ce_scope_zones =
	    args.scopeZones === null ? null : args.scopeZones.slice(0);
	this.ce_scope_services =
	    args.scopeServices === null ? null : args.scopeServices.slice(0);
	this.ce_scope_cns = args.scopeComputeNodes;
	this.ce_scope_cns = args.scopeComputeNodes === null ? null :
	    args.scopeComputeNodes.slice(0);
	this.ce_scope_globals = args.scopeGlobalZones;

	this.ce_scope_allowed_services = {};
	madm.maSvcNamesOnEach.forEach(
	    function (s) { self.ce_scope_allowed_services[s] = true; });

	/*
	 * Command execution parameters
	 */
	assertplus.ok(args.execMode == MZ_EM_COMMAND ||
	    args.execMode == MZ_EM_SENDTOREMOTE ||
	    args.execMode == MZ_EM_RECEIVEFROMREMOTE, 'args.execMode');
	this.ce_exec_mode = args.execMode;

	assertplus.number(args.execTimeout, 'args.execTimeout');
	this.ce_exec_timeout = args.execTimeout;

	if (this.ce_exec_mode == MZ_EM_COMMAND) {
		assertplus.string(args.execCommand, 'args.execCommand');
		this.ce_exec_command = args.execCommand;
		assertplus.ok(args.execDirectory === null,
		    'args.execDirectory must not be specified for commands');
		this.ce_exec_dir = null;
		assertplus.ok(args.execFile === null,
		    'args.execFile must not be specified for commands');
		this.ce_exec_file = null;
		assertplus.ok(args.execClobber === null,
		    'args.execClobber must not be specified for commands');
		this.ce_exec_clobber = null;
	} else {
		assertplus.ok(args.execCommand === null,
		    'args.execCommand must not be specified for file ' +
		    'transfers');
		this.ce_exec_command = null;
		assertplus.string(args.execDirectory, 'args.execDirectory');
		this.ce_exec_dir = args.execDirectory;
		assertplus.string(args.execFile, 'args.execFile');
		this.ce_exec_file = args.execFile;
		assertplus.optionalBool(args.execClobber, 'args.execClobber');
		this.ce_exec_clobber = args.execClobber ? true : false;
		assertplus.optionalString(args.bindIp, 'args.bindIp');
		this.ce_bindip = args.bindIp;
	}

	/*
	 * Other parameters
	 */
	assertplus.object(args.streamStatus, 'args.streamStatus');
	assertplus.object(args.streamStatus, 'args.log');
	assertplus.bool(args.dryRun, 'args.dryRun');
	assertplus.number(args.concurrency, 'args.concurrency');

	this.ce_stream = args.streamStatus;
	this.ce_log = args.log;
	this.ce_dryrun = args.dryRun;
	this.ce_concurrency = args.concurrency;


	/*
	 * Helper objects
	 */
	this.ce_urclient = null;	/* client for Ur facility */
	this.ce_ur_ready = null;	/* time when we connected to AMQP */
	this.ce_manta = null;		/* MantaAdm object */
	this.ce_pipeline = null;	/* vasync pipeline for operation */
	this.ce_queue = null;		/* vasync queue for Ur commands */

	/*
	 * Internal state
	 */
	this.ce_started = null;		/* Date when we started reading */

	/*
	 * Set of servers, by server_uuid.  Each server has:
	 *
	 *     s_hostname       (string) server hostname
	 *     s_server_uuid	(string) server uuid
	 *     s_cmds		(array)  list of zones assigned to this server,
	 *                               each as a "command".  Each has
	 *                               properties:
	 *
	 *         cmd_server_uuid  (string) server where this command is to run
	 *         cmd_service      (string) name of SAPI service for this zone
	 *         cmd_zonename     (string) name of this zone
	 *         cmd_command      (string) actual command to execute
	 *                                   (null if args.scopeGlobalZones is
	 *                                   true)
	 *         cmd_result       (object) describes result of each command
	 *
	 *     s_result		(object) result of command on this server
	 *                               (only when args.scopeGlobalZones is
	 *                               true)
	 */
	this.ce_servers = null;

	/* counter for operations started */
	this.ce_nstarted = 0;
	/* counter for operations completed, successfully or otherwise */
	this.ce_ncompleted = 0;
	/*
	 * counter for number of operations failed
	 * These represent failures to execute the command (e.g., failures at
	 * Ur, like a timeout), not cases where the command itself exited with a
	 * non-zero status or was killed.
	 */
	this.ce_nexecerrors = 0;

	stream.Readable.call(this, { 'objectMode': true });

	this.ce_log.info({
	    'amqpTimeout': args.amqpTimeout,
	    'amqpHost': args.amqpHost,
	    'amqpPort': args.amqpPort,
	    'amqpLogin': args.amqpLogin,
	    'amqpPassword': args.amqpPassword,
	    'sdcMantaConfigFile': args.sdcMantaConfigFile,
	    'scopeAllZones': args.scopeAllZones,
	    'scopeZones': args.scopeZones,
	    'scopeServices': args.scopeServices,
	    'scopeComputeNodes': args.scopeComputeNodes,
	    'scopeGlobalZones': args.scopeGlobalZones,
	    'execTimeout': args.execTimeout,
	    'execMode': args.execMode,
	    'execCommand': args.execCommand,
	    'execFile': args.execFile,
	    'execDirectory': args.execFile,
	    'execClobber': args.execClobber,
	    'bindIp': args.bindIp,
	    'concurrency': args.concurrency,
	    'dryRun': args.dryRun
	}, 'command executor created');
}

util.inherits(mzCommandExecutor, stream.Readable);

/*
 * The first call to _read() begins executing the operation defined by the
 * configuration passed into the constructor.  This stream does not support flow
 * control, so we ignore all future calls to _read().
 */
mzCommandExecutor.prototype._read = function ()
{
	var self = this;
	var i, funcs;

	if (this.ce_started !== null) {
		return;
	}

	this.ce_started = new Date();
	assertplus.ok(this.ce_pipeline === null,
	    'CommandExecutor.execute() cannot be invoked more than once');

	/*
	 * To enable users to use shell features (including operators like "&&",
	 * redirections, and parameter expansion), we pass their script to bash
	 * on stdin (rather than using "bash -c").  In order to do that from our
	 * own shell script (which is the primitive that Ur provides us), we use
	 * a heredoc using an EOF delimiter that we expect never to see in a
	 * user's script.  If we see this marker in the user's script, we'll
	 * detect that and bail out with an explicit error.
	 *
	 * Needless to say, we expect this would never happen outside of a
	 * deliberate test.  (The most plausible scenario is somebody's bash
	 * script containing a chunk of this file itself, presumably in some way
	 * that won't be interpreted as bash code, but this does not seem a
	 * critical use-case to support.)  If this becomes a problem, we could
	 * generate random marker strings until we find one that's not contained
	 * in the user's script.
	 */
	if (this.ce_exec_mode == MZ_EM_COMMAND &&
	    this.ce_exec_command.indexOf(mzScriptEofMarker) != -1) {
		setImmediate(this.emit.bind(this), 'error',
		    new VError('unsupported command (contains our marker)'));
		return;
	}

	/*
	 * Proactively check for operation on disallowed services.
	 */
	if (this.ce_scope_services !== null) {
		for (i = 0; i < this.ce_scope_services.length; i++) {
			if (!this.ce_scope_allowed_services[
			    this.ce_scope_services[i]]) {
				setImmediate(this.emit.bind(this), 'error',
				    new VError('unsupported service: "%s"',
				    this.ce_scope_services[i]));
				return;
			}
		}
	}

	funcs = [];

	if (this.ce_amqp_host === null ||
	    this.ce_amqp_login === null ||
	    this.ce_amqp_password === null ||
	    this.ce_amqp_timeout === null) {
		funcs.push(this.stageConfigAmqp.bind(this));
	}

	funcs.push(this.stageSetupManta.bind(this));

	/*
	 * If we're doing a file transfer and the user did not specify a binding
	 * IP address, then add the pipeline stage to autoconfigure it.
	 */
	if ((this.ce_exec_mode == MZ_EM_SENDTOREMOTE ||
	    this.ce_exec_mode == MZ_EM_RECEIVEFROMREMOTE) &&
	    this.ce_bindip === null) {
		funcs.push(this.stageSetupBindIp.bind(this));
	}

	funcs.push(this.stageSetupUr.bind(this));
	funcs.push(this.stageIdentifyScope.bind(this));

	if (this.ce_dryrun) {
		funcs.push(this.stageDryrunCommands.bind(this));
	} else {
		funcs.push(this.stageExecuteCommands.bind(this));
	}

	this.ce_pipeline = vasync.pipeline({
	    'funcs': funcs,
	    'arg': this
	}, function (err) {
		assertplus.equal(self.ce_nstarted, self.ce_ncompleted);
		self.close();

		if (err) {
			self.ce_log.error(err, 'error');
			self.emit('error', err);
		} else {
			/* Emit 'end'. */
			self.ce_log.info('done');
			self.push(null);
		}
	});
};

/*
 * Returns the number of failures to execute a command on a remote host.  These
 * represent Ur-level failures or other failures not related to the command
 * itself.  If the command exited non-zero or was killed, that will not be
 * reflected here.
 */
mzCommandExecutor.prototype.nexecerrors = function ()
{
	return (this.ce_nexecerrors);
};

/*
 * This first stage reads AMQP configuration from the local SDC configuration
 * file.  This is only used when the AMQP configuration is not already fully
 * specified (as by command-line arguments).
 */
mzCommandExecutor.prototype.stageConfigAmqp = function (_, callback)
{
	var self = this;

	assertplus.string(this.ce_sdc_config_path);

	self.ce_log.trace({
	    'path': this.ce_sdc_config_path
	}, 'loading amqp config from file');

	sdc.sdcReadAmqpConfig(this.ce_sdc_config_path, function (err, config) {
		if (err) {
			callback(new VError(err, 'auto-configuring AMQP'));
			return;
		}

		self.ce_log.debug(config, 'loaded amqp config');

		/*
		 * There's currently no way for the port, login, or password to
		 * be specified by the configuration.  We could add it to the
		 * configuration file, but this information is not provided by
		 * SAPI, so it would be hardcoded in the config template anyway.
		 */
		assertplus.string(config.host);
		if (self.ce_amqp_host === null) {
			self.ce_amqp_host = config.host;
		}

		callback();
	});
};

/*
 * Setup pipeline stage for initializing a MantaAdm client and fetching all
 * state about the current Manta deployment.
 */
mzCommandExecutor.prototype.stageSetupManta = function (_, callback)
{
	var self = this;

	assertplus.ok(this.ce_manta === null);

	this.ce_manta = new madm.MantaAdm(
	    this.ce_log.child({ 'component': 'MantaAdm' }));

	this.ce_manta.loadSdcConfig(function (err) {
		if (err) {
			callback(new VError(err, 'manta-adm load sdc config'));
			return;
		}

		self.ce_manta.fetchDeployed(function (err2) {
			if (err2) {
				callback(new VError(err2,
				    'manta-adm fetch deployed'));
				return;
			}

			self.ce_log.debug('MantaAdm data loaded');
			callback();
		});
	});
};

/*
 * Determines an appropriate binding IP address to use for the local HTTP server
 * that's used for file transfers.
 */
mzCommandExecutor.prototype.stageSetupBindIp = function (_, callback)
{
	var self = this;
	var zonename, args;

	assertplus.ok(this.ce_exec_mode != MZ_EM_COMMAND);
	assertplus.ok(this.ce_bindip === null);

	this.ce_log.debug('determing local bind IP for file transfer');

	args = {};
	zonename = zoneutil.getzonename();
	if (zonename != 'global') {
		args['belongs_to_type'] = 'zone';
		args['belongs_to_uuid'] = zonename;
		this.pickBindIpFor(args, callback);
		return;
	}

	args['belongs_to_type'] = 'server';
	forkexec.forkExecWait({
	    'argv': [ 'sysinfo' ]
	}, function (err, result) {
		var parsed;

		if (err) {
			callback(new VError(err, 'failed to select local IP'));
			return;
		}

		try {
			parsed = JSON.parse(result.stdout);
		} catch (ex) {
			callback(new VError(ex,
			    'failed to select local IP: parsing sysinfo'));
			return;
		}

		if (typeof (parsed['UUID']) != 'string' ||
		    parsed['UUID'].length === 0) {
			callback(new VError('failed to select local IP: ' +
			    'failed to extract UUID from sysinfo'));
			return;
		}

		args['belongs_to_uuid'] = parsed['UUID'];
		self.pickBindIpFor(args, callback);
	});
};

/*
 * Finds an IP address suitable for binding a local HTTP server in order to
 * execute file transfer operations.  This IP address must be:
 *
 *    (1) accessible only to trusted users (i.e., on a private network), since
 *        the local HTTP server uses no authentication nor encryption.  This
 *        isn't much of a problem because the contexts where this is run are
 *        usually already trusted contexts and not on public networks.
 *
 *    (2) accessible in the current context.  It's important that we support
 *        operation from the global zone of an SDC headnode which may not itself
 *        have an IP on the "manta" network.
 *
 *    (3) accessible on the context of any Manta component.
 *
 * This pretty much means we have to pick our local IP on the SDC "admin"
 * network.
 */
mzCommandExecutor.prototype.pickBindIpFor = function (args, callback)
{
	var self = this;

	assertplus.object(args, 'args');
	assertplus.string(args.belongs_to_type, 'args.belongs_to_type');
	assertplus.string(args.belongs_to_uuid, 'args.belongs_to_uuid');

	this.ce_manta.findAdminIpForComponent({
	    'belongs_to_type': args.belongs_to_type,
	    'belongs_to_uuid': args.belongs_to_uuid
	}, function (err, ip) {
		if (err) {
			callback(err);
			return;
		}

		self.ce_bindip = ip;
		callback();
	});
};

/*
 * Setup pipeline stage for initializing our Ur client (for managing remote
 * execution and file transfer).
 */
mzCommandExecutor.prototype.stageSetupUr = function (_, callback)
{
	var self = this;
	var amqp;

	assertplus.string(this.ce_amqp_host);
	assertplus.number(this.ce_amqp_port);
	assertplus.string(this.ce_amqp_login);
	assertplus.string(this.ce_amqp_password);

	amqp = {
	    'host': this.ce_amqp_host,
	    'port': this.ce_amqp_port,
	    'login': this.ce_amqp_login,
	    'password': this.ce_amqp_password
	};

	this.ce_log.debug(amqp, 'amqp config');
	assertplus.ok(this.ce_urclient === null);
	this.ce_urclient = urclient.create_ur_client({
	    'log': this.ce_log.child({ 'component': 'UrClient' }),
	    'connect_timeout': this.ce_amqp_timeout,
	    'enable_http': this.ce_exec_mode != MZ_EM_COMMAND,
	    'bind_ip': this.ce_bindip !== null ? this.ce_bindip : null,
	    'amqp_config': amqp
	});

	this.ce_urclient.on('ready', function () {
		self.ce_log.debug('ur client ready');
		self.ce_ur_ready = new Date();
		callback();
	});

	this.ce_urclient.on('error', function (err) {
		callback(new VError(err, 'Ur client'));
	});
};

/*
 * Now that we've fetched details about all deployed Manta components, use the
 * scope parameters specified in the constructor to identify exactly which
 * commands need to be executed where.
 */
mzCommandExecutor.prototype.stageIdentifyScope = function (_, callback)
{
	var self = this;
	var err, count;

	this.ce_log.trace('identifying scope of operations');

	assertplus.ok(this.ce_servers === null);
	this.ce_servers = {};

	count = 0;
	err = this.ce_manta.eachZoneByFilter({
	    'scopeZones': this.ce_scope_zones,
	    'scopeServices': this.ce_scope_services,
	    'scopeComputeNodes': this.ce_scope_cns
	}, function (zoneinfo) {
		if (zoneinfo['GZ HOST'] == '-') {
			self.ce_log.trace(zoneinfo,
			    'ignoring zone (appears to be in a different DC');
			return;
		}

		if (!self.ce_scope_allowed_services[zoneinfo['SERVICE']]) {
			self.ce_log.trace(zoneinfo,
			    'ignoring zone (disallowed service)');
			return;
		}

		/*
		 * "marlin" is not one of the whitelisted services, but
		 * double-check before we do anything else, since it's not safe
		 * to log into those zones while jobs may be running.
		 */
		assertplus.ok(zoneinfo['SERVICE'] != 'marlin');
		self.ce_log.trace(zoneinfo, 'found zone');

		if (!self.ce_servers.hasOwnProperty(
		    zoneinfo['SERVER_UUID'])) {
			self.ce_servers[zoneinfo['SERVER_UUID']] = {
			    's_hostname': zoneinfo['GZ HOST'],
			    's_server_uuid': zoneinfo['SERVER_UUID'],
			    's_cmds': [],
			    's_result': null
			};
		}

		self.ce_servers[zoneinfo['SERVER_UUID']].s_cmds.push({
		    'cmd_server_uuid': zoneinfo['SERVER_UUID'],
		    'cmd_service': zoneinfo['SERVICE'],
		    'cmd_zonename': zoneinfo['ZONENAME'],
		    'cmd_command': self.ce_exec_mode == MZ_EM_COMMAND ?
		        self.makeUrScript(zoneinfo['ZONENAME']) : null,
		    'cmd_result': null
		});

		count++;
	});

	if (!err && count === 0) {
		err = new VError('no matching zones found');
	}

	setImmediate(callback, err);
};

/*
 * Now that we've figured out which commands need to be executed where, go and
 * actually execute them.
 */
mzCommandExecutor.prototype.stageDryrunCommands = function (_, callback)
{
	var self = this;

	assertplus.ok(this.ce_dryrun);
	if (this.ce_scope_globals) {
		jsprim.forEachKey(this.ce_servers, function (s) {
			if (self.ce_exec_mode == MZ_EM_COMMAND) {
				fprintf(self.ce_stream, 'host %s: %s', s,
				    self.ce_exec_command);
			} else {
				fprintf(self.ce_stream,
				    'host %s: file transfer', s);
			}
		});
	} else {
		jsprim.forEachKey(this.ce_servers, function (s, server) {
			var cmds = server.s_cmds;
			fprintf(self.ce_stream, 'host %s: %d command%s\n',
			    s, cmds.length, cmds.length == 1 ? '' : 's');
			cmds.forEach(function (cmd) {
				if (self.ce_exec_mode == MZ_EM_COMMAND) {
					fprintf(self.ce_stream,
					    '    zone %s: %s\n',
					    cmd.cmd_zonename,
					    self.ce_exec_command);
				} else {
					fprintf(self.ce_stream,
					    '    zone %s: file transfer\n',
					    cmd.cmd_zonename);
				}
			});
		});
	}

	fprintf(self.ce_stream,
	    '\nLeave off -n / --dry-run to execute.\n');
	setImmediate(callback);
};

/*
 * Now that we've figured out which commands need to be executed where, go and
 * actually execute them.
 */
mzCommandExecutor.prototype.stageExecuteCommands = function (_, callback)
{
	var queue;

	assertplus.ok(!this.ce_dryrun);
	assertplus.ok(this.ce_queue === null);

	this.ce_log.debug('begin execution');

	queue = this.ce_queue = vasync.queuev({
	    'concurrency': this.ce_concurrency,
	    'worker': this.queueExecuteOne.bind(this)
	});

	queue.on('drain', function () { callback(); });

	if (this.ce_scope_globals) {
		jsprim.forEachKey(this.ce_servers, function (s) {
			queue.push(s);
		});
	} else {
		jsprim.forEachKey(this.ce_servers, function (s, server) {
			server.s_cmds.forEach(function (cmd) {
				queue.push(cmd);
			});
		});
	}

	queue.close();
};

/*
 * Cleans up any resources that we may be holding onto so that the process can
 * exit normally.
 */
mzCommandExecutor.prototype.close = function ()
{
	this.ce_log.debug('close');

	if (this.ce_manta !== null) {
		this.ce_manta.close();
	}

	if (this.ce_urclient !== null) {
		this.ce_urclient.close();
	}
};

/*
 * Construct an appropriate Ur script for the script that we're supposed to run
 * inside each zone.
 */
mzCommandExecutor.prototype.makeUrScript = function (zonename)
{
	var script;

	/*
	 * This function is only used to generate scripts to be executed within
	 * a non-global zone.  If we're operating on global zones, the returned
	 * script will not be used.
	 */
	if (this.ce_scope_globals) {
		return (null);
	}

	/*
	 * We've already validated this earlier, but make sure the script does
	 * not contain our own EOF marker.
	 */
	assertplus.equal(this.ce_exec_mode, MZ_EM_COMMAND);
	assertplus.equal(this.ce_exec_command.indexOf(mzScriptEofMarker), -1);

	/*
	 * Also make sure that our zonename does not contain anything other than
	 * the very restricted character set with which we create zonenames.  We
	 * want to be sure there will be no surprising behavior if the zonename
	 * was allowed to contain characters that are special to bash.
	 */
	assertplus.ok(/^[a-zA-Z0-9-]+/.test(zonename));

	/*
	 * This might be cleaner with sprintf() or with an external template,
	 * but then we'd have to escape characters in the user's script.  Stick
	 * with simple concatenation.  The reason we handle "113" specially is
	 * that Ur interprets this exit status to mean "reboot the server when
	 * complete", and we want to avoid that particular land mine in
	 * "manta-oneach".
	 */
	script = 'cat << \'' + mzScriptEofMarker + '\' | ' +
	        '/usr/sbin/zlogin -Q ' + zonename + ' bash -l\n' +
	    this.ce_exec_command + '\n' +
	    mzScriptEofMarker + '\n' +
	    'rv=$?\n' +
	    'if [[ $rv -eq 113 ]]; then exit 1; else exit $rv ; fi';
	return (script);
};

/*
 * Given the execution arguments specified in the constructor, execute the
 * requested operation.  Results are placed back into "this".
 */
mzCommandExecutor.prototype.queueExecuteOne = function (cmd, qcallback)
{
	if (this.ce_exec_mode == MZ_EM_COMMAND) {
		this.queueExecuteOneCommand(cmd, qcallback);
	} else {
		this.queueExecuteOneTransfer(cmd, qcallback);
	}
};

/*
 * Implementation of queueExecuteOne() when we're executing an arbitrary
 * command.
 */
mzCommandExecutor.prototype.queueExecuteOneCommand = function (cmd, qcallback)
{
	var self = this;
	var urargs, summary;

	if (this.ce_scope_globals) {
		urargs = {
		    'server_uuid': cmd,
		    'timeout': this.ce_exec_timeout,
		    'script': this.ce_exec_command,
		    'env': {
			'PATH': process.env['PATH']
		    }
		};
	} else {
		urargs = {
		    'server_uuid': cmd.cmd_server_uuid,
		    'timeout': this.ce_exec_timeout,
		    'script': cmd.cmd_command
		};
	}

	summary = this.urStart('ur exec start', urargs, cmd);
	this.ce_urclient.exec(urargs, function (err, result) {
		self.urDone('ur exec done', cmd, summary, err, result);
		qcallback();
	});
};

/*
 * Implementation of queueExecuteOne() for file transfers.
 */
mzCommandExecutor.prototype.queueExecuteOneTransfer = function (cmd, qcallback)
{
	var func, urargs, summary, patherr;
	var self = this;

	urargs = {};
	if (this.ce_scope_globals) {
		urargs['server_uuid'] = cmd;
	} else {
		urargs['server_uuid'] = cmd.cmd_server_uuid;
	}

	urargs['timeout'] = this.ce_exec_timeout;

	if (this.ce_exec_mode == MZ_EM_SENDTOREMOTE) {
		func = this.ce_urclient.send_file;
		urargs['src_file'] = this.ce_exec_file;
		urargs['clobber'] = this.ce_exec_clobber;

		if (this.ce_scope_globals) {
			urargs['dst_dir'] = this.ce_exec_dir;
		} else {
			urargs['dst_dir'] = path.join('/zones',
			    cmd.cmd_zonename, 'root', this.ce_exec_dir);
			patherr = this.validateZonePath(urargs['dst_dir'],
			    cmd.cmd_zonename);
		}
	} else {
		func = this.ce_urclient.recv_file;

		if (this.ce_scope_globals) {
			urargs['dst_file'] = path.join(
			    this.ce_exec_dir, urargs['server_uuid']);
			urargs['src_file'] = this.ce_exec_file;
		} else {
			urargs['dst_file'] = path.join(
			    this.ce_exec_dir, cmd.cmd_zonename);
			urargs['src_file'] = path.join('/zones',
			    cmd.cmd_zonename, 'root', this.ce_exec_file);
			patherr = this.validateZonePath(urargs['src_file'],
			    cmd.cmd_zonename);
		}
	}

	summary = this.urStart('file transfer start', urargs, cmd);
	if (patherr instanceof Error) {
		patherr = new VError(patherr, 'cannot start transfer');
		self.urDone('file transfer done', cmd, summary, patherr, null);
		setImmediate(qcallback);
		return;
	}

	func.call(this.ce_urclient, urargs, function onTransferDone(err) {
		self.urDone('file transfer done', cmd, summary, err,
		    err ? null : {
			'exit_status': 0,
			'stdout': 'ok',
			'stderr': ''
		    });
		qcallback();
	});
};

/*
 * Given a path ZONEPATH that will be interpreted in the global zone of a
 * compute node, check that it appears to land inside the root filesystem for
 * zone ZONENAME.  This is used to check for user error, not to implement
 * security.  See the notes in the documentation on why the "oneach" mechanism
 * must only be used by trusted users.  (In this case, there's no way for us to
 * tell ahead of time whether the path references a symlink that would point
 * outside the zone we're targeting.  We'd have to re-implement file transfer
 * using a mechanism that executes inside the zone in order to do that securely,
 * but this is an SDC operator tool, and users with access to it cannot escalate
 * privileges as a result of this design choice.)
 *
 * This function assumes the convention of SmartOS containers that the zone's
 * root filesystem is mounted at /zones/ZONENAME/root in the global zone.
 */
mzCommandExecutor.prototype.validateZonePath = function (zonepath, zonename)
{
	var normalized_parts;

	normalized_parts = path.normalize(zonepath).split('/');
	if (normalized_parts.length < 4 ||
	    normalized_parts[0] !== '' ||
	    normalized_parts[1] !== 'zones' ||
	    normalized_parts[2] != zonename ||
	    normalized_parts[3] != 'root') {
		return (new VError(
		    'path "%s" is not contained inside zone "%s"',
		    zonepath, zonename));
	}

	return (null);
};

/*
 * Common code for starting an Ur operation.  This bumps counters, logs what
 * we're doing, and returns a summary that can be augmented and later passed to
 * urDone().
 */
mzCommandExecutor.prototype.urStart = function (label, urargs, cmd)
{
	var summary;

	assertplus.string(label, 'label');
	assertplus.object(urargs, 'urargs');

	this.ce_log.debug(urargs, label);
	this.ce_nstarted++;

	summary = {
	    'uuid': urargs['server_uuid'],
	    'hostname': this.ce_servers[urargs['server_uuid']].s_hostname
	};

	if (!this.ce_scope_globals) {
		summary['zonename'] = cmd.cmd_zonename;
		summary['service'] = cmd.cmd_service;
	}

	return (summary);
};

/*
 * Common code for completing an Ur operation.  This bumps couners, logs what
 * we've done, and records the result of the oeration into "this".
 */
mzCommandExecutor.prototype.urDone = function (label, cmd, summary,
    err, result)
{
	var internalResult;

	assertplus.string(label, 'label');
	assertplus.object(summary, 'summary');

	this.ce_ncompleted++;

	if (err) {
		this.ce_nexecerrors++;
		summary['error'] = err;
	} else {
		summary['result'] = result;
	}

	internalResult = {
	    'ur_err': err,
	    'ur_result': result
	};

	if (this.ce_scope_globals) {
		assertplus.ok(this.ce_servers[cmd].s_result === null);
		this.ce_servers[cmd].s_result = internalResult;
	} else {
		assertplus.ok(cmd.cmd_result === null);
		cmd.cmd_result = internalResult;
	}

	this.ce_log.debug(summary, label);
	this.push(summary);
};


/*
 * Result formatters
 */


/*
 * Transform stream that takes results from mzCommandExecutor and writes them in
 * a text format suitable for interactive use.  Named options include:
 *
 *     omitHeader    (boolean)	Skip the header row that's used for one-line
 *     				output.
 *
 *     outputBatch   (boolean)  If true, then the results are batched up until
 *     				all of them have been received.  The results are
 *     				then sorted by the target (zone or compute node)
 *     				so that they appear in a consistent order across
 *     				runs.  Finally, the results are printed.
 *
 *     				If false, then results are printed as they
 *     				arrive.
 *
 *     multilineMode (enum)	One of "one", "multi", or "auto".
 *
 *		"one" means that the output for each result is collapsed into a
 *		single line (by taking the last non-empty line) and the results
 *		are printed, one line per result.
 *
 *		"multi" means that the complete output is printed for each
 *		result.  This may consist of several lines.  Each result is
 *		prefaced with a header line.
 *
 *		"auto" uses a heuristic to determine whether "one" or "multi"
 *		should be used.  "one" is used if every result contains no more
 *		than one non-empty line, and "multi" is used otherwise.  This
 *		mode cannot be used unless outputBatch is true.
 */
function mzResultToText(args)
{
	assertplus.object(args, 'args');
	assertplus.bool(args.omitHeader, 'args.omitHeader');
	assertplus.bool(args.outputBatch, 'args.outputBatch');
	assertplus.string(args.multilineMode, 'args.multilineMode');
	assertplus.ok(args.multilineMode == 'one' ||
	    args.multilineMode == 'multi' || args.multilineMode == 'auto');

	stream.Transform.call(this, {
	    'objectMode': true,
	    'highWatermark': 16
	});

	this.rt_omit_header = args.omitHeader;
	this.rt_batch = args.outputBatch;
	this.rt_mode = args.multilineMode;
	this.rt_auto = 'one';

	this.rt_done_header = false;
	this.rt_results = this.rt_batch ? [] : null;
}

util.inherits(mzResultToText, stream.Transform);

mzResultToText.prototype._transform = function (summary, _, callback)
{
	if (this.rt_batch) {
		if (this.rt_mode == 'auto' && this.rt_auto == 'one' &&
		    this.outputForEntry(summary).split('\n').length > 2) {
			this.rt_auto = 'multi';
		}

		this.rt_results.push(summary);
	} else {
		this.printResult(summary, this.rt_mode);
	}

	setImmediate(callback);
};

mzResultToText.prototype.outputForEntry = function (summary)
{
	var output;

	if (summary['error']) {
		output = sprintf('ERROR: %s\n', summary['error'].message);
	} else {
		/*
		 * We adopt sdc-oneachnode's convention of reporting stdout for
		 * successful commands, and stdout + stderr for failed commands.
		 */
		output = summary['result']['stdout'];
		if (summary['result']['exit_status'] !== 0) {
			output += summary['result']['stderr'];
		}
	}

	return (output);
};

mzResultToText.prototype.printResult = function (summary, mode)
{
	var label, output, trailer, lines;

	if (!this.rt_omit_header && !this.rt_done_header && mode === 'one') {
		this.rt_done_header = true;

		if (summary['zonename']) {
			this.push(sprintf('%-16s %-8s %s\n',
			    'SERVICE', 'ZONE', 'OUTPUT'));
		} else {
			this.push(sprintf('%-22s%s\n', 'HOSTNAME', 'OUTPUT'));
		}
	}

	if (summary['zonename']) {
		if (mode == 'multi') {
			label = sprintf('=== Output from %s on %s (%s):\n',
			    summary['zonename'], summary['hostname'],
			    summary['service']);
		} else {
			label = sprintf('%-16s %s ', summary['service'],
			    summary['zonename'].substr(0, 8));
		}
	} else {
		if (mode == 'multi') {
			/* This matches sdc-oneachnode's default output. */
			label = sprintf('=== Output from %s (%s):\n',
			    summary['uuid'], summary['hostname']);
		} else {
			label = sprintf('%-22s', summary['hostname']);
		}
	}

	/*
	 * In one-line mode, we take only the last non-empty line.  When
	 * this mode was selected automatically, this is equivalent to
	 * the whole string, but if this mode was specifically requested
	 * by the user then this causes us to present only the last line
	 * of output.
	 */
	output = this.outputForEntry(summary);
	if (mode != 'multi') {
		lines = output.split('\n').filter(
		    function (l) { return (l.length > 0); });
		if (lines.length === 0) {
			output = '\n';
		} else {
			output = lines[lines.length - 1] + '\n';
		}
	}

	trailer = mode == 'multi' ? '\n' : '';
	this.push(label + output + trailer);
};

mzResultToText.prototype._flush = function (callback)
{
	var self = this;
	var mode;

	if (this.rt_batch) {
		if (this.rt_mode == 'auto') {
			mode = this.rt_auto;
		} else {
			mode = this.rt_mode;
		}

		assertplus.ok(mode == 'one' || mode == 'multi');

		this.rt_results.sort(function (r1, r2) {
			var r;

			if (r1.hasOwnProperty('service')) {
				r = r1['service'].localeCompare(r2['service']);
				if (r !== 0) {
					return (r);
				}

				r = r1['zonename'].localeCompare(
				    r2['zonename']);
				if (r !== 0)
					return (r);
			}

			r = r1['hostname'].localeCompare(r2['hostname']);
			return (r);
		}).forEach(function (r) {
			self.printResult(r, mode);
		});
	}

	setImmediate(callback);
};

/*
 * Transform stream that takes results from mzCommandExecutor and writes them
 * directly in JSON form.  This version never batches results.
 */
function mzResultToJson()
{
	stream.Transform.call(this, {
	    'objectMode': true,
	    'highWatermark': 16
	});
}

util.inherits(mzResultToJson, stream.Transform);

mzResultToJson.prototype._transform = function (summary, _, callback)
{
	this.push(JSON.stringify(summary) + '\n');
	setImmediate(callback);
};


/*
 * Given scope parameters that will be passed to mzCommandExecutor(), validate
 * that they're supported.  Returns an Error describing any failure, or null if
 * the parameters are valid.
 *
 * Note that type errors and the like are considered programmer errors: it's the
 * responsibility of the caller to construct a syntactically valid set of
 * parameters.  This function validates the semantics.
 */
function mzValidateScopeParameters(args)
{
	var havefilters;

	assertplus.optionalArrayOfString(args.scopeZones, 'args.scopeZones');
	assertplus.optionalArrayOfString(args.scopeServices,
	    'args.scopeServices');
	assertplus.optionalArrayOfString(args.scopeComputeNodes,
	    'args.scopeComputeNodes');
	assertplus.bool(args.scopeGlobalZones, 'args.scopeGlobalZones');
	assertplus.bool(args.scopeAllZones, 'args.scopeAllZones');

	havefilters = args.scopeComputeNodes !== null ||
	    args.scopeZones !== null || args.scopeServices !== null;

	if (args.scopeAllZones && havefilters) {
		return (new VError('cannot specify zones, services, ' +
		    'or compute nodes when all zones were requested'));
	}

	if (!args.scopeAllZones && !havefilters) {
		return (new VError('must explicitly request all zones ' +
		    'to operate on all zones'));
	}

	return (null);
}
