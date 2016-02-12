#!/usr/bin/env node
/* vim: set ft=javascript: */
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * manta-adm.js: manage manta deployments.  Provides subcommands:
 *
 *     cn		show information about CNs
 *
 *     show		show information about all deployed services
 *
 *     update		given a JSON file describing the desired Manta
 *     			configuration, figure out how to get from the current
 *     			state to the desired state, and start taking those
 *     			actions
 *
 *     genconfig	generate a configuration suitable for a single-system
 *     			install on COAL or a lab machine or based on a
 *     			configuration file describing available resources
 *
 *     zk		view and manage configured nameserver instances
 *
 * NOTE: this file contains ONLY the CLI wrapper around the real functionality
 * contained in lib/adm.js.  Do NOT add deployment logic here.  It belongs in
 * the library that can eventually be consumed by other tools.
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var cmdln = require('cmdln');
var cmdutil = require('cmdutil');
var jsprim = require('jsprim');
var path = require('path');
var util = require('util');
var vasync = require('vasync');
var VError = require('verror').VError;
var common = require('../lib/common');
var deploy = require('../lib/deploy');
var madm = require('../lib/adm');

var maArg0 = path.basename(process.argv[1]);

/*
 * node-cmdln interface for the manta-adm tool.
 */
function MantaAdm()
{
	cmdln.Cmdln.call(this, {
	    'name': maArg0,
	    'desc': 'Inspect and modify deployed Manta services'
	});
}

util.inherits(MantaAdm, cmdln.Cmdln);

MantaAdm.prototype.initAdm = function (opts, callback)
{
	var logstreams;

	if (opts.log_file == 'stdout') {
		logstreams = [ {
		    'level': 'debug',
		    'stream': process.stdout
		} ];
	} else if (opts.log_file) {
		logstreams = [ {
		    'level': 'debug',
		    'path': opts.log_file
		} ];
		console.error('logs at ' + opts.log_file);
	} else {
		logstreams = [ {
		    'level': 'fatal',
		    'stream': process.stderr
		} ];
	}

	this.madm_log = new bunyan({
	    'name': maArg0,
	    'streams': logstreams,
	    'serializers': bunyan.stdSerializers
	});

	this.madm_adm = new madm.MantaAdm(this.madm_log);
	this.madm_adm.loadSdcConfig(function (err) {
		if (err)
			fatal(err.message);
		callback();
	});
};

MantaAdm.prototype.finiAdm = function ()
{
	this.madm_adm.close();
};

MantaAdm.prototype.do_cn = function (subcmd, opts, args, callback)
{
	var self = this;
	var options = {};
	var selected;

	if (opts.columns) {
		selected = checkColumns(madm.cnColumnNames(), opts.columns);
		if (selected instanceof Error) {
			callback(selected);
			return;
		}

		options.columns = selected;
	}

	if (opts.storage_only)
		options.onlystorage = true;

	if (opts.oneachnode)
		options.oneachnode = true;

	if (opts.omit_header)
		options.omitHeader = true;

	if (args.length > 0)
		options.filter = args[0];

	if (args.length > 1) {
		callback(new Error('unexpected arguments'));
		return;
	}

	this.initAdm(opts, function () {
		var adm = self.madm_adm;
		adm.fetchDeployed(function (err) {
			if (err)
				fatal(err.message);

			adm.dumpCns(process.stdout, options);
			self.finiAdm();
		});
	});
};

MantaAdm.prototype.do_cn.help =
    'Show information about Manta servers in this DC.\n\n' +
    'Usage:\n\n' +
    '    manta-adm cn OPTIONS [FILTER]\n\n' +
    'Examples:\n\n' +
    '    # list basic info about all Manta CNs in this DC\n' +
    '    manta-adm cn\n\n' +
    '    # list info about Manta CN with server uuid matching 7432ffc8\n' +
    '    manta-adm cn 7432ffc8\n\n' +
    '    # list only storage nodes\n' +
    '    manta-adm cn -s\n\n' +
    '    # list only the hostnames (and omit the header)\n' +
    '    manta-adm cn -H -o host\n\n' +
    '    # list hostnames in form suitable for "sdc-oneachnode -n"\n' +
    '    manta-adm cn -n\n\n' +
    'FILTER may be any substring of the compute node\'s server uuid, \n' +
    'admin IP, hostname, compute id, or storage ids.\n\n' +
    '{{options}}\n' +
    'Available columns for -o:\n    ' + madm.cnColumnNames().join(', ');

MantaAdm.prototype.do_cn.options = [ {
    'names': [ 'omit-header', 'H'],
    'type': 'bool',
    'help': 'Omit the header row for columnar output'
}, {
    'names': [ 'log_file', 'l' ],
    'type': 'string',
    'help': 'Dump logs to this file (or "stdout")'
}, {
    'names': [ 'oneachnode', 'n' ],
    'type': 'bool',
    'help': 'Emit output suitable for "sdc-oneachnode -n"'
}, {
    'names': [ 'columns', 'o' ],
    'type': 'arrayOfString',
    'help': 'Select columns for output (see below)'
}, {
    'names': [ 'storage-only', 's' ],
    'type': 'bool',
    'help': 'Show only nodes used as storage nodes.'
}];

MantaAdm.prototype.do_genconfig = function (subcmd, opts, args, callback)
{
	var self = this;
	var fromfile = opts.from_file;

	if (fromfile) {
		if (args.length !== 0) {
			callback(new Error('unexpected arguments'));
			return;
		}
	} else if (args.length != 1 ||
	    (args[0] != 'lab' && args[0] != 'coal')) {
		callback(new Error(
		    'expected "lab", "coal", or --from-file option'));
		return;
	}

	this.initAdm(opts, function () {
		var adm = self.madm_adm;
		var func;
		var options = {
		    'outstream': process.stdout
		};

		if (args[0] == 'lab') {
			func = adm.dumpConfigLab;
		} else if (args[0] == 'coal') {
			func = adm.dumpConfigCoal;
		} else {
			assertplus.string(fromfile);
			func = adm.genconfigFromFile;
			options['filename'] = fromfile;
			options['errstream'] = process.stderr;
		}

		adm.fetchDeployed(function (err) {
			if (err)
				fatal(err.message);

			func.call(adm, options, function (serr, nwarnings) {
				if (serr)
					fatal(serr.message);

				if (nwarnings !== 0) {
					console.error('error: bailing out ' +
					    'because of at least one issue');
					process.exit(1);
				}
				self.finiAdm();
			});
		});
	});
};

MantaAdm.prototype.do_genconfig.help =
    'Generate a configuration for COAL or lab deployment or for \n' +
    'a larger deployment.\n' +
    '\n' +
    'Usage:\n' +
    '\n' +
    '    manta-adm genconfig lab\n' +
    ' or manta-adm genconfig coal\n' +
    ' or manta-adm genconfig --from-file=FILE\n';

MantaAdm.prototype.do_genconfig.options = [ {
    'names': [ 'from-file' ],
    'type': 'string',
    'helpArg': 'FILE',
    'help': 'Use server descriptions in FILE'
} ];

/*
 * manta-adm show: shows information about deployed services
 */
MantaAdm.prototype.do_show = function (subcmd, opts, args, callback)
{
	var self = this;
	var selected, filter;

	if ((opts.bycn || opts.all) && opts.summary) {
		callback(new Error('-c and -a cannot be used with -s'));
		return;
	}

	if (opts.json && (!opts.summary || opts.omit_header)) {
		callback(new Error('-j cannot be used without -s or with -H'));
		return;
	}

	if (opts.columns) {
		selected = checkColumns(madm.columnNames(), opts.columns);
		if (selected instanceof Error) {
			callback(selected);
			return;
		}
	}

	if (args.length > 1) {
		callback(new Error('unexpected arguments'));
		return;
	}

	if (args.length > 0)
		filter = args[0];

	this.initAdm(opts, function () {
		var adm, func;
		adm = self.madm_adm;
		if (!opts.summary) {
			if (opts.bycn)
				func = adm.dumpDeployedZonesByCn;
			else
				func = adm.dumpDeployedZonesByService;
		} else if (opts.json) {
			func = adm.dumpDeployedConfigByServiceJson;
		} else {
			func = adm.dumpDeployedConfigByService;
		}

		adm.fetchDeployed(function (err) {
			if (err)
				fatal(err.message);

			func.call(adm, process.stdout, {
			    'doall': opts.all,
			    'omitHeader': opts.omit_header,
			    'filter': filter,
			    'columns': opts.columns ? selected : null
			});
			self.finiAdm();
		});
	});
};

MantaAdm.prototype.do_show.help =
    'Show information about deployed services.\n\n' +
    'Usage:\n\n' +
    '    manta-adm show OPTIONS [SERVICE]\n\n' +
    'Examples:\n\n' +
    '    # list all Manta zones in the current DC\n' +
    '    manta-adm show\n\n' +
    '    # list zones in the current DC by compute node\n' +
    '    manta-adm show -c\n\n' +
    '    # summarize Manta zones in the current DC\n' +
    '    manta-adm show -s\n\n' +
    '    # list all Manta zones in all datacenters (no IP info available)\n' +
    '    manta-adm show -a\n\n' +
    '    # show only postgres zones in the current datacenter\n' +
    '    manta-adm show postgres\n\n' +
    '{{options}}\n' +
    'Available columns for -o:\n    ' + madm.columnNames().join(', ');

MantaAdm.prototype.do_show.options = [ {
    'names': [ 'all', 'a' ],
    'type': 'bool',
    'help': 'Show results from all datacenters, rather than just the local one'
}, {
    'names': [ 'bycn', 'c' ],
    'type': 'bool',
    'help': 'Show results by compute node, rather than by service.'
}, {
    'names': [ 'omit-header', 'H'],
    'type': 'bool',
    'help': 'Omit the header row for columnar output'
}, {
    'names': [ 'json', 'j' ],
    'type': 'bool',
    'help': 'Show results in JSON form suitable for importing with "update".'
}, {
    'names': [ 'log_file', 'l' ],
    'type': 'string',
    'help': 'dump logs to this file (or "stdout")'
}, {
    'names': [ 'columns', 'o' ],
    'type': 'arrayOfString',
    'help': 'Select columns for output (see below)'
}, {
    'names': [ 'summary', 's' ],
    'type': 'bool',
    'help': 'Show summary of deployed zones rather than each zone separately.'
} ];

/*
 * manta-adm update: deploys, undeploys, and redeploys to match a desired
 * deployment specification
 */
MantaAdm.prototype.do_update = function (subcmd, opts, args, callback)
{
	var filename, service, nchanges, adm;
	var self = this;

	if (args.length === 0) {
		callback(new Error(
		    'expected filename for desired configuration'));
		return;
	}

	if (args.length > 2) {
		callback(new Error('unexpected arguments'));
		return;
	}

	filename = args[0];
	if (args.length == 2)
		service = args[1];

	vasync.pipeline({
	    'funcs': [
		function initAdm(_, stepcb) {
			self.initAdm(opts, function () {
				adm = self.madm_adm;
				stepcb();
			});
		},
		function readConfig(_, stepcb) {
			adm.readConfigFromFile(filename, stepcb);
		},
		function fetchDeployed(_, stepcb) {
			adm.fetchDeployed(stepcb);
		},
		function generatePlan(_, stepcb) {
			adm.generatePlan(stepcb, service, opts.no_reprovision);
		},
		function dumpPlan(_, stepcb) {
			adm.execPlan(process.stdout, process.stderr,
			    true, function (err, count) {
				if (err) {
					stepcb(err);
					return;
				}

				nchanges = count;
				if (count > 0 && opts.dryrun)
					console.log('To apply these changes, ' +
					    'leave off -n (--dry-run).');

				stepcb();
			    });
		},
		function uconfirm(_, stepcb) {
			if (opts.dryrun || nchanges === 0 || opts.confirm) {
				stepcb();
				return;
			}

			common.confirm(
			    'Are you sure you want to proceed? (y/N): ',
			    function (proceed) {
				process.stdout.write('\n');
				if (!proceed) {
					stepcb(new Error('aborted by user'));
				} else {
					stepcb();
				}
			    });
		},
		function execPlan(_, stepcb) {
			if (opts.dryrun || nchanges === 0) {
				stepcb();
				return;
			}

			adm.execPlan(process.stdout, process.stderr,
			    false, stepcb);
		}
	    ]
	}, function (err) {
		if (err)
			fatal(err.message);
		self.finiAdm();
		callback();
	});
};

MantaAdm.prototype.do_update.help =
    'Update deployment to match a JSON configuration.\n\n{{options}}';

MantaAdm.prototype.do_update.options = [ {
    'names': [ 'log_file', 'l' ],
    'type': 'string',
    'help': 'dump logs to this file (or "stdout")',
    'default': '/var/log/manta-adm.log'
}, {
    'names': [ 'dryrun', 'n' ],
    'type': 'bool',
    'help': 'Print what would be done without actually doing it.'
}, {
    'names': [ 'confirm', 'y' ],
    'type': 'bool',
    'help': 'Bypass all confirmations (be careful!)'
}, {
    'names': [ 'no-reprovision' ],
    'type': 'bool',
    'help': 'When upgrading a zone, always provision and deprovision ' +
	'rather than reprovision'
} ];

MantaAdm.prototype.do_zk = MantaAdmZk;

function MantaAdmZk(parent)
{
	this.mn_parent = parent;
	cmdln.Cmdln.call(this, {
	    'name': 'zk',
	    'desc': 'View and modify ZooKeeper servers configuration.'
	});
}

util.inherits(MantaAdmZk, cmdln.Cmdln);

MantaAdmZk.prototype.do_list = function (subcmd, opts, args, callback)
{
	var self = this;
	var options = {};
	var selected;

	if (args.length > 0) {
		callback(new Error('unexpected arguments'));
		return;
	}

	if (opts.columns) {
		selected = checkColumns(madm.zkColumnNames(), opts.columns);
		if (selected instanceof Error) {
			callback(selected);
			return;
		}

		options.columns = selected;
	}

	if (opts.omit_header)
		options.omitHeader = true;

	this.mn_parent.initAdm(opts, function () {
		var adm = self.mn_parent.madm_adm;
		adm.fetchDeployed(function (err) {
			var problems;

			if (err)
				fatal(err.message);
			problems = adm.dumpZkServers(process.stdout, options);
			problems.critical.forEach(function (warn) {
				console.error('error: %s', warn.message);
			});
			problems.fixable.forEach(function (warn) {
				console.error('warning: %s', warn.message);
			});

			if (problems.critical.length +
			    problems.fixable.length > 0)
				process.exit(1);
			self.mn_parent.finiAdm();
		});
	});
};

MantaAdmZk.prototype.do_list.help =
    'List configured ZooKeeper servers\n\n' +
    'Usage:\n\n' +
    '    manta-adm zk list OPTIONS\n\n' +
    'Examples:\n\n' +
    '    # list ZooKeeper servers\n' +
    '    manta-adm zk list\n\n' +
    '    # list only IPs of ZK servers\n' +
    '    manta-adm zk list --omit-header -o ip\n\n' +
    '{{options}}\n' +
    'Available columns for -o:\n    ' + madm.zkColumnNames().join(', ');

/*
 * Note that the "manta-adm" commands that may modify the system use
 * /var/log/manta-adm.log as the default log file, as those logs currently serve
 * as general debug logs.  But the "zk list" subcommand is read-only and only
 * applicable to this user, so we use a path in /var/tmp for the log.
 */
MantaAdmZk.prototype.do_list.options = [ {
    'names': [ 'omit-header', 'H'],
    'type': 'bool',
    'help': 'Omit the header row for columnar output'
}, {
    'names': [ 'log_file', 'l' ],
    'type': 'string',
    'help': 'dump logs to this file (or "stdout")',
    'default': '/var/tmp/manta-adm.log'
}, {
    'names': [ 'columns', 'o' ],
    'type': 'arrayOfString',
    'help': 'Select columns for output (see below)'
} ];

MantaAdmZk.prototype.do_fixup = function (subcmd, opts, args, callback)
{
	var self = this;
	var adm, nissues, nfixed;

	if (args.length > 0) {
		callback(new Error('unexpected arguments'));
		return;
	}

	vasync.pipeline({
	    'funcs': [
		function initAdm(_, stepcb) {
			self.mn_parent.initAdm(opts, function () {
				adm = self.mn_parent.madm_adm;
				stepcb();
			});
		},
		function fetchDeployed(_, stepcb) {
			adm.fetchDeployed(stepcb);
		},
		function dumpNameservers(_, stepcb) {
			var problems;

			console.error('CURRENT CONFIGURATION');
			problems = adm.dumpZkServers(process.stderr, {});
			if (problems.critical.length > 0) {
				problems.critical.forEach(function (e) {
					console.error('error: %s', e.message);
				});
				stepcb(new VError('bailing out after errors'));
				return;
			}

			nissues = problems.fixable.length;
			if (nissues === 0) {
				console.error('no issues to repair');
				stepcb();
				return;
			}

			console.error(
			    'The following issues should be repaired:');
			problems.fixable.forEach(function (e) {
				console.error('error: %s', e.message);
			});

			if (opts.dryrun) {
				console.error('To repair, leave off ' +
				    '-n (--dry-run)');
			}

			stepcb();
		},
		function uconfirm(_, stepcb) {
			if (opts.dryrun || nissues === 0 || opts.confirm) {
				stepcb();
				return;
			}

			common.confirm(
			    'Do you want to repair these issues now? (y/N): ',
			    function (proceed) {
				process.stdout.write('\n');
				if (!proceed) {
					stepcb(new Error('aborted by user'));
				} else {
					stepcb();
				}
			    });
		},
		function repair(_, stepcb) {
			if (opts.dryrun || nissues === 0) {
				stepcb();
				return;
			}

			adm.fixupZkServers(function (err, n) {
				if (!err)
					nfixed = n;
				stepcb(err);
			});
		}
	    ]
	}, function (err) {
		if (err)
			fatal(err.message);
		if (!opts.dryrun && nissues > 0)
			console.error('%d issue%s repaired',
			    nfixed, nfixed == 1 ? '' : 's');
		self.mn_parent.finiAdm();
		callback();
	});
};

MantaAdmZk.prototype.do_fixup.help = [
    'Repair ZooKeeper configuration',
    '',
    'This command compares the ZooKeeper configuration (defined by the ',
    'ZK_SERVERS and ZK_ID SAPI metadata properties) to the list of deployed ',
    'nameservice zones, reports any discrepancies or other issues, and ',
    'optionally repairs certain kinds of issues.  If repairs are made, only ',
    'metadata is changed.  This tool is intended for cases where a ZK server ',
    'has been undeployed and the configuration needs to be updated, or where ',
    'deployment failed and left stale configuration, or other unusual cases ',
    'where the configuration does not match the list of deployed nameservers.',
    'The "manta-adm zk list" command identifies these problem cases.',
    '',
    'Usage:',
    '',
    '    manta-adm zk [-n | --dry-run] [-y | --confirm] fixup',
    '',
    'Examples:',
    '',
    '    # check for configuration issues and repair them',
    '    manta-adm zk fixup',
    '',
    '{{options}}'
].join('\n');

MantaAdmZk.prototype.do_fixup.options = [ {
    'names': [ 'confirm', 'y' ],
    'type': 'bool',
    'help': 'Bypass all confirmations (be careful!)'
}, {
    'names': [ 'dryrun', 'n' ],
    'type': 'bool',
    'help': 'Print what would be done without actually doing it.'
}, {
    'names': [ 'log_file', 'l' ],
    'type': 'string',
    'help': 'Dump logs to this file (or "stdout")'
} ];

function checkColumns(allowed, columns)
{
	var selected, c, i;

	selected = [];
	for (i = 0; i < columns.length; i++) {
		c = columns[i].split(',');
		selected = selected.concat(columns[i].split(','));
	}

	for (i = 0; i < selected.length; i++) {
		c = selected[i];
		if (allowed.indexOf(c) == -1)
			return (new VError('unknown column: "%s"', c));
	}

	return (selected);
}

function fatal(msg)
{
	console.error('%s: %s', maArg0, msg);
	process.exit(1);
}

cmdutil.exitOnEpipe();
cmdln.main(MantaAdm);
