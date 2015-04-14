#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
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
 *     			install on either COAL or a lab machine
 *
 * In the long term, this could be used for initial deployment as well as
 * upgrades (including mass upgrades, as for the Marlin compute zones).  For
 * now, this is a prototype that can be used for the Marlin compute zones.
 *
 * NOTE: this file contains ONLY the CLI wrapper around the real functionality
 * contained in lib/adm.js.  Do NOT add deployment logic here.  It belongs in
 * the library that can eventually be consumed by other tools.
 */

var bunyan = require('bunyan');
var cmdln = require('cmdln');
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

	if (args.length > 1)
		callback(new Error('unexpected arguments'));

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
	if (args.length != 1 || (args[0] != 'lab' && args[0] != 'coal')) {
		callback(new Error('expected "lab" or "coal"'));
		return;
	}

	var self = this;
	this.initAdm(opts, function () {
		var adm = self.madm_adm;
		var func = args[0] == 'lab' ?
		    adm.dumpConfigLab : adm.dumpConfigCoal;

		adm.fetchDeployed(function (err) {
			if (err)
				fatal(err.message);

			func.call(adm, process.stdout,
			    function (serr, nwarnings) {
				if (serr)
					fatal(serr.message);

				if (nwarnings !== 0) {
					console.error('error: %d services ' +
					    'were not included', nwarnings);
					process.exit(nwarnings);
				}
				self.finiAdm();
			    });
		});
	});
};

MantaAdm.prototype.do_genconfig.help =
    'Generate a configuration for a COAL or lab deployment.\n' +
    '\n' +
    'Usage:\n' +
    '\n' +
    '    manta-adm genconfig lab\n' +
    ' or manta-adm genconfig coal\n';

MantaAdm.prototype.do_genconfig.options = [];

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

process.stdout.on('error', function (err) {
	if (err.code == 'EPIPE')
		process.exit(0);
	throw (err);
});
cmdln.main(MantaAdm);
