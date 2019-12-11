#!/usr/bin/env node
/* vim: set ft=javascript: */
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * manta-adm.js: manage manta deployments.  Provides subcommands:
 *
 *     alarm		view and configure information about alarms
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
 *     accel-gc         configure accelerated garbage-collection
 *
 *     create-topology  generate a hash ring for electric-moray
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
var restifyClients = require('restify-clients');
var util = require('util');
var vasync = require('vasync');
var VError = require('verror').VError;
var common = require('../lib/common');
var madm = require('../lib/adm');

var maArg0 = path.basename(process.argv[1]);

var maDefaultAlarmConcurrency = 10;

/*
 * We will warn when the user attempts to create a maintenance window that's
 * longer than this value (in milliseconds).  This is currently 4 hours.
 */
var maMaintWindowLong = 4 * 60 * 60 * 1000;

/*
 * These node-cmdln options are used by multiple subcommands.  They're defined
 * in one place to ensure consistency in names, aliases, and help message.
 */
var maCommonOptions = {
    columns: {
        names: ['columns', 'o'],
        type: 'arrayOfString',
        help: 'Select columns for output (see below).'
    },
    concurrency: {
        names: ['concurrency'],
        type: 'positiveInteger',
        help: 'Number of concurrent requests to make.',
        default: maDefaultAlarmConcurrency
    },
    configFile: {
        names: ['config-file'],
        type: 'string',
        help: 'Path to configuration.',
        default: common.CONFIG_FILE_DEFAULT
    },
    confirm: {
        names: ['confirm', 'y'],
        type: 'bool',
        help: 'Bypass all confirmations (be careful!)'
    },
    dryrun: {
        names: ['dryrun', 'n'],
        type: 'bool',
        help: 'Print what would be done without actually doing it.'
    },
    logFile: {
        names: ['log_file', 'log-file', 'l'],
        type: 'string',
        help: 'Dump logs to this file (or "stdout").',
        default: '/var/log/manta-adm.log'
    },
    logFileDefaultNone: {
        names: ['log_file', 'log-file', 'l'],
        type: 'string',
        help: 'Dump logs to this file (or "stdout")'
    },
    omitHeader: {
        names: ['omit-header', 'H'],
        type: 'bool',
        help: 'Omit the header row for columnar output.'
    },
    unconfigure: {
        names: ['unconfigure'],
        type: 'bool',
        help: 'Remove all probes and probe groups instead of updating them.',
        default: false
    }
};

/*
 * node-cmdln interface for the manta-adm tool.
 */
function MantaAdm() {
    cmdln.Cmdln.call(this, {
        name: maArg0,
        desc: 'Inspect and modify deployed Manta services'
    });
}

util.inherits(MantaAdm, cmdln.Cmdln);

/*
 * Performs common initialization steps used by most subcommands.  "opts" are
 * the cmdln-parsed CLI options.  This function processes the "log_file" option.
 */
MantaAdm.prototype.initAdm = function(opts, callback) {
    var logstreams;

    if (opts.log_file === 'stdout') {
        logstreams = [
            {
                level: 'debug',
                stream: process.stdout
            }
        ];
    } else if (opts.log_file) {
        logstreams = [
            {
                level: 'debug',
                path: opts.log_file
            }
        ];
        console.error('logs at ' + opts.log_file);
    } else {
        logstreams = [
            {
                level: process.env['LOG_LEVEL'] || 'fatal',
                stream: process.stderr
            }
        ];
    }

    this.madm_log = new bunyan({
        name: maArg0,
        streams: logstreams,
        serializers: restifyClients.bunyan.serializers
    });

    this.madm_adm = new madm.MantaAdm(this.madm_log);
    this.madm_adm.loadSdcConfig(function(err) {
        if (err) {
            fatal(err.message);
        }
        callback();
    });
};

MantaAdm.prototype.finiAdm = function() {
    this.madm_adm.close();
};

MantaAdm.prototype.do_alarm = MantaAdmAlarm;

MantaAdm.prototype.do_cn = function(_subcmd, opts, args, callback) {
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

    if (opts.storage_only) {
        options.onlystorage = true;
    }

    if (opts.oneachnode) {
        options.oneachnode = true;
    }

    if (opts.omit_header) {
        options.omitHeader = true;
    }

    if (args.length > 0) {
        options.filter = args[0];
    }

    if (args.length > 1) {
        callback(new Error('unexpected arguments'));
        return;
    }

    this.initAdm(opts, function() {
        var adm = self.madm_adm;
        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

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
    "FILTER may be any substring of the compute node's server uuid, \n" +
    'admin IP, hostname, compute id, or storage ids.\n\n' +
    '{{options}}\n' +
    'Available columns for -o:\n    ' +
    madm.cnColumnNames().join(', ');

MantaAdm.prototype.do_cn.options = [
    maCommonOptions.omitHeader,
    maCommonOptions.logFileDefaultNone,
    {
        names: ['oneachnode', 'n'],
        type: 'bool',
        help: 'Emit output suitable for "sdc-oneachnode -n"'
    },
    maCommonOptions.columns,
    {
        names: ['storage-only', 's'],
        type: 'bool',
        help: 'Show only nodes used as storage nodes.'
    }
];

function MantaAdmAccelGc(parent) {
    this.magc_parent = parent;
    cmdln.Cmdln.call(this, {
        name: parent.name + ' gc',
        desc: 'Configure accelerated garbage-collection.'
    });
}
util.inherits(MantaAdmAccelGc, cmdln.Cmdln);

MantaAdm.prototype.do_accel_gc = MantaAdmAccelGc;

MantaAdmAccelGc.prototype.do_show = function(_subcmd, opts, args, callback) {
    var self = this;

    if (args.length > 1) {
        callback(new Error('unexpected arguments'));
        return;
    }

    self.magc_parent.initAdm(opts, function() {
        var adm, func;
        adm = self.magc_parent.madm_adm;

        if (opts.json) {
            func = adm.dumpDeployedInstanceMetadataJson;
        } else {
            callback(
                new Error(
                    'human-readable format ' +
                        'not yet supported, use -j to print json'
                )
            );
            return;
        }

        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

            func.call(adm, process.stdout, process.stderr, {
                doall: opts.all,
                svcname: 'garbage-collector',
                fields: common.GC_METADATA_FIELDS
            });
            self.magc_parent.finiAdm();
            callback();
        });
    });
};

MantaAdmAccelGc.prototype.do_show.help =
    'Output a JSON object mapping of metadata shards to ' +
    'garbage-collectors.\n\n' +
    'Usage:\n\n' +
    '    manta-adm accel-gc show\n\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_show.options = [
    {
        names: ['all', 'a'],
        type: 'bool',
        help:
            'Show results from all datacenters, rather than just ' +
            'the local one'
    },
    {
        names: ['json', 'j'],
        type: 'bool',
        help:
            'Show results in JSON form suitable for passing to ' +
            '"manta-adm accel-gc update".'
    }
];

MantaAdmAccelGc.prototype.do_update = function(_subcmd, opts, args, callback) {
    var self, adm;
    var filename;

    self = this;

    if (args.length === 0) {
        callback(new Error('expected filename for desired configuration'));
        return;
    }

    if (args.length > 1) {
        callback(new Error('unexpected arguments'));
        return;
    }

    filename = args[0];

    vasync.pipeline(
        {
            funcs: [
                function initAdm(_, stepcb) {
                    assertplus.ok(opts !== null);
                    self.magc_parent.initAdm(opts, function() {
                        adm = self.magc_parent.madm_adm;
                        stepcb();
                    });
                },
                function fetchDeployed(_, stepcb) {
                    adm.fetchDeployed(stepcb);
                },
                function readConfig(_, stepcb) {
                    adm.readInstanceMetadataConfigFromFile(filename, stepcb);
                },
                function execUpdate(_, stepcb) {
                    adm.updateDeployedInstanceMetadata(
                        {
                            svcname: 'garbage-collector'
                        },
                        stepcb
                    );
                }
            ]
        },
        function(err) {
            if (err) {
                fatal(err.message);
            }
            self.magc_parent.finiAdm();
            callback();
        }
    );
};

MantaAdmAccelGc.prototype.do_update.help =
    'Update the mapping of index shards to garbage-collectors.\n\n' +
    'Usage:\n\n' +
    '    manta-adm accel-gc update [OPTIONS] CONFIG-FILE\n\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_update.options = [];

MantaAdmAccelGc.prototype.do_gen_shard_assignment = function(
    _subcmd,
    opts,
    _args,
    callback
) {
    var self = this;

    self.magc_parent.initAdm(opts, function() {
        var adm = self.magc_parent.madm_adm;
        var func = adm.genGcMetadataConfig;
        var options = {
            outstream: process.stdout
        };

        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

            func.call(adm, options, function(ferr) {
                if (ferr) {
                    fatal(ferr.message);
                }
                self.magc_parent.finiAdm();
                callback();
            });
        });
    });
};

MantaAdmAccelGc.prototype.do_gen_shard_assignment.help =
    'Generate an assignment of index shards to deployed ' +
    'garbage-collector instances.\n\n' +
    '    manta-adm accel-gc gen-shard-assignment\n\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_gen_shard_assignment.options = [];

MantaAdmAccelGc.prototype.do_genconfig = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var num_collectors, max_cns;
    var avoid_svcs;
    var imageuuid;
    var self;

    if (args.length !== 2) {
        callback(new Error('missing arguments: IMAGE_UUID NCOLLECTORS'));
        return;
    }

    self = this;
    imageuuid = args[0];

    num_collectors = jsprim.parseInteger(args[1], {base: 10});
    if (num_collectors instanceof Error) {
        callback(
            new VError(
                num_collectors,
                'unable to generate garbage-collector deployment config'
            )
        );
        return;
    }

    max_cns = opts.max_cns;
    avoid_svcs = opts.ignore_criteria
        ? []
        : opts.avoid_svcs || ['loadbalancer', 'nameservice', 'storage'];

    self.magc_parent.initAdm(opts, function() {
        var adm = self.magc_parent.madm_adm;
        var func = adm.layerServiceOnDeployedConfig;

        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

            func.call(adm, {
                avoid_svcs: avoid_svcs,
                image_uuid: imageuuid,
                max_cns: max_cns,
                num_instances: num_collectors,
                outstream: process.stdout,
                errstream: process.stderr,
                svcname: 'garbage-collector'
            });

            self.magc_parent.finiAdm();
            callback();
        });
    });
};

MantaAdmAccelGc.prototype.do_genconfig.help =
    'Layer a number of garbage-collector instances on top of an existing ' +
    'Manta deployment.\n\n' +
    '    manta-adm accel-gc genconfig [OPTIONS] IMAGE_UUID NCOLLECTORS\n\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_genconfig.options = [
    {
        names: ['max-cns', 'm'],
        type: 'integer',
        helpArg: 'MAX_CNS',
        help:
            'The number of CNs on which to distribute collector instances. ' +
            'If this option is not specified the collector instances will ' +
            'be distributed on as many viable CNs as possible'
    },
    {
        names: ['avoid-svcs', 'a'],
        type: 'arrayOfString',
        help:
            'Avoid co-locating garbage-collector instances with the given ' +
            'service. Specify multiple services by repeating this option: ' +
            "'-a loadbalancer -a nameservice'. By default, this command " +
            'avoids co-locating services with loadbalancer, nameservice, ' +
            'and storage instances.'
    },
    {
        names: ['ignore-criteria', 'i'],
        type: 'bool',
        help:
            'Ignore service co-location constraints, which may not be ' +
            'satisfiable in all deployments.'
    }
];

MantaAdmAccelGc.prototype.do_enable = function(_subcmd, opts, args, callback) {
    var self;
    var account;

    if (args.length !== 1) {
        callback(new Error('missing arguments: ACCOUNT-LOGIN'));
        return;
    }

    self = this;
    account = args[0];

    if (account === 'poseidon') {
        callback(new Error('accelerated gc is not supported for poseidon'));
        return;
    }

    self.magc_parent.initAdm(opts, function() {
        var adm = self.magc_parent.madm_adm;

        adm.fetchDeployed(function(err) {
            if (err) {
                callback(err);
                return;
            }
            var options = {
                account: account
            };
            adm.disableSnaplinks(options, function(disableErr) {
                if (disableErr) {
                    fatal(disableErr.message);
                }
                self.magc_parent.finiAdm();
                callback(disableErr);
            });
        });
    });
};

MantaAdmAccelGc.prototype.do_enable.help =
    'Enable accelerated garbage-collection for an account.\n' +
    'This also disables snaplinks for the account.\n\n' +
    '    manta-adm accel-gc enable [ACCOUNT-LOGIN]\n\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_enable.options = [];

MantaAdmAccelGc.prototype.do_disable = function(_subcmd, opts, args, callback) {
    var self;
    var account;

    if (args.length !== 1) {
        callback(new Error('missing arguments: ACCOUNT-LOGIN'));
        return;
    }

    self = this;
    account = args[0];

    if (account === 'poseidon') {
        callback(new Error('accelerated gc is not supported for poseidon'));
        return;
    }

    self.magc_parent.initAdm(opts, function() {
        var adm = self.magc_parent.madm_adm;

        adm.fetchDeployed(function(err) {
            if (err) {
                callback(err);
                return;
            }
            var options = {
                account: account
            };
            adm.enableSnaplinks(options, function(enableErr) {
                if (enableErr) {
                    fatal(enableErr.message);
                }
                self.magc_parent.finiAdm();
                callback(enableErr);
            });
        });
    });
};

MantaAdmAccelGc.prototype.do_disable.help =
    'Disable accelerated garbage-collection for an account.\n' +
    'This enables snaplinks for the account.\n\n' +
    '    manta-adm accel-gc disable [ACCOUNT-LOGIN]\n\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_disable.options = [];

MantaAdmAccelGc.prototype.do_accounts = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var self, options;

    self = this;
    options = {};

    if (args.length !== 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    options = listPrepareArgs(opts, madm.gcColumnNames());
    if (options instanceof Error) {
        callback(options);
        return;
    }
    options.stream = process.stdout;

    self.magc_parent.initAdm(opts, function() {
        var adm = self.magc_parent.madm_adm;

        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

            adm.dumpSnaplinkDisabledAccounts(options, function(outputErr) {
                if (err) {
                    fatal(outputErr.message);
                }
                self.magc_parent.finiAdm();
                callback(outputErr);
            });
        });
    });
};

MantaAdmAccelGc.prototype.do_accounts.help =
    'List accounts using accelerated garbage-collection.\n\n' +
    '    manta-adm accel-gc accounts [OPTIONS]\n' +
    '{{options}}';

MantaAdmAccelGc.prototype.do_accounts.options = [
    maCommonOptions.omitHeader,
    maCommonOptions.columns
];

MantaAdm.prototype.do_genconfig = function(_subcmd, opts, args, callback) {
    var self = this;
    var fromfile = opts.from_file;

    if (fromfile) {
        if (args.length !== 0) {
            callback(new Error('unexpected arguments'));
            return;
        }
    } else if (args.length !== 1 || (args[0] !== 'lab' && args[0] !== 'coal')) {
        callback(new Error('expected "lab", "coal", or --from-file option'));
        return;
    } else if (opts.directory) {
        callback(new Error('--directory can only be used with --from-file'));
        return;
    }

    this.initAdm(opts, function() {
        var adm = self.madm_adm;
        var func;
        var options = {};

        if (args[0] === 'lab') {
            func = adm.dumpConfigLab;
            options['outstream'] = process.stdout;
        } else if (args[0] === 'coal') {
            func = adm.dumpConfigCoal;
            options['outstream'] = process.stdout;
        } else {
            assertplus.string(fromfile);
            func = adm.genconfigFromFile;
            options['filename'] = fromfile;
            if (opts.directory) {
                options['outDirectory'] = opts.directory;
            } else {
                options['outstream'] = process.stdout;
            }
            options['errstream'] = process.stderr;
        }

        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

            func.call(adm, options, function(serr, nissues) {
                if (serr) {
                    fatal(serr.message);
                }

                if (nissues !== 0) {
                    console.error(
                        'error: bailing out because of at least one issue'
                    );
                    process.exit(1);
                }

                self.finiAdm();
            });
        });
    });
};

MantaAdm.prototype.do_genconfig.help =
    'Generate a config for COAL, lab, or a larger deployment.\n' +
    '\n' +
    'Usage:\n' +
    '\n' +
    '    manta-adm genconfig lab\n' +
    ' or manta-adm genconfig coal\n' +
    ' or manta-adm genconfig [--directory DIR] --from-file=FILE\n';

MantaAdm.prototype.do_genconfig.options = [
    {
        names: ['from-file'],
        type: 'string',
        helpArg: 'FILE',
        help: 'Use server descriptions in FILE'
    },
    {
        names: ['directory', 'd'],
        type: 'string',
        helpArg: 'DIR',
        help: 'Output directory for generated configs'
    }
];

/*
 * manta-adm show: shows information about deployed services
 */
MantaAdm.prototype.do_show = function(_subcmd, opts, args, callback) {
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

    if (args.length > 0) {
        filter = args[0];
    }

    this.initAdm(opts, function() {
        var adm, func;
        adm = self.madm_adm;
        if (!opts.summary) {
            if (opts.bycn) {
                func = adm.dumpDeployedZonesByCn;
            } else {
                func = adm.dumpDeployedZonesByService;
            }
        } else if (opts.json) {
            func = adm.dumpDeployedConfigByServiceJson;
        } else {
            func = adm.dumpDeployedConfigByService;
        }

        adm.fetchDeployed(function(err) {
            if (err) {
                fatal(err.message);
            }

            func.call(adm, process.stdout, {
                doall: opts.all,
                omitHeader: opts.omit_header,
                filter: filter,
                columns: opts.columns ? selected : null
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
    'Available columns for -o:\n    ' +
    madm.columnNames().join(', ');

MantaAdm.prototype.do_show.options = [
    {
        names: ['all', 'a'],
        type: 'bool',
        help:
            'Show results from all datacenters, rather than just the local one'
    },
    {
        names: ['bycn', 'c'],
        type: 'bool',
        help: 'Show results by compute node, rather than by service.'
    },
    maCommonOptions.omitHeader,
    {
        names: ['json', 'j'],
        type: 'bool',
        help: 'Show results in JSON form suitable for importing with "update".'
    },
    maCommonOptions.logFileDefaultNone,
    maCommonOptions.columns,
    {
        names: ['summary', 's'],
        type: 'bool',
        help: 'Show summary of deployed zones rather than each zone separately.'
    }
];

/*
 * manta-adm update: deploys, undeploys, and redeploys to match a desired
 * deployment specification
 */
MantaAdm.prototype.do_update = function(_subcmd, opts, args, callback) {
    var filename, service, nchanges, adm;
    var self = this;

    if (args.length === 0) {
        callback(new Error('expected filename for desired configuration'));
        return;
    }

    if (args.length > 2) {
        callback(new Error('unexpected arguments'));
        return;
    }

    filename = args[0];
    if (args.length === 2) {
        service = args[1];
    }

    vasync.pipeline(
        {
            funcs: [
                function initAdm(_, stepcb) {
                    self.initAdm(opts, function() {
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
                function determineDefaultChannel(_, stepcb) {
                    if (opts.skip_verify_channel) {
                        stepcb();
                        return;
                    }
                    adm.determineSdcChannel(stepcb);
                },
                function generatePlan(_, stepcb) {
                    adm.generatePlan(
                        {
                            service: service,
                            noreprovision: opts.no_reprovision,
                            experimental: opts.experimental
                        },
                        stepcb
                    );
                },
                function verifyPlan(_, stepcb) {
                    adm.verifyPlan(
                        {
                            skip_verify_channel: opts.skip_verify_channel
                        },
                        stepcb
                    );
                },
                function dumpPlan(_, stepcb) {
                    adm.execPlan(process.stdout, process.stderr, true, function(
                        err,
                        count
                    ) {
                        if (err) {
                            stepcb(err);
                            return;
                        }

                        nchanges = count;
                        if (count > 0 && opts.dryrun) {
                            console.log(
                                'To apply these changes, ' +
                                    'leave off -n (--dry-run).'
                            );
                        }

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
                        function(proceed) {
                            process.stdout.write('\n');
                            if (!proceed) {
                                stepcb(new Error('aborted by user'));
                            } else {
                                stepcb();
                            }
                        }
                    );
                },
                function execPlan(_, stepcb) {
                    if (opts.dryrun || nchanges === 0) {
                        stepcb();
                        return;
                    }

                    adm.execPlan(process.stdout, process.stderr, false, stepcb);
                }
            ]
        },
        function(err) {
            if (err) {
                fatal(err.message);
            }
            self.finiAdm();
            callback();
        }
    );
};

MantaAdm.prototype.do_update.help =
    'Update deployment to match a JSON configuration.\n\n' +
    'Usage:\n\n' +
    '    manta-adm update [OPTIONS] CONFIG-FILE [SERVICE]\n\n' +
    '{{options}}';

MantaAdm.prototype.do_update.options = [
    maCommonOptions.logFile,
    maCommonOptions.dryrun,
    maCommonOptions.confirm,
    {
        names: ['no-reprovision'],
        type: 'bool',
        help:
            'When upgrading a zone, always provision and deprovision ' +
            'rather than reprovision'
    },
    {
        names: ['experimental', 'X'],
        type: 'bool',
        help: 'Allow deployment of experimental services'
    },
    {
        names: ['skip-verify-channel'],
        type: 'bool',
        help:
            'When provisioning an image, avoid verifying that this image ' +
            'comes from the default update channel for this datacenter',
        default: false
    }
];

MantaAdm.prototype.do_zk = MantaAdmZk;

function MantaAdmZk(parent) {
    this.mn_parent = parent;
    cmdln.Cmdln.call(this, {
        name: parent.name + ' zk',
        desc: 'View and modify ZooKeeper servers configuration.'
    });
}

util.inherits(MantaAdmZk, cmdln.Cmdln);

MantaAdmZk.prototype.do_list = function(_subcmd, opts, args, callback) {
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

    if (opts.omit_header) {
        options.omitHeader = true;
    }

    this.mn_parent.initAdm(opts, function() {
        var adm = self.mn_parent.madm_adm;
        adm.fetchDeployed(function(err) {
            var problems;

            if (err) {
                fatal(err.message);
            }
            problems = adm.dumpZkServers(process.stdout, options);
            problems.critical.forEach(function(warn) {
                console.error('error: %s', warn.message);
            });
            problems.fixable.forEach(function(warn) {
                console.error('warning: %s', warn.message);
            });

            if (problems.critical.length + problems.fixable.length > 0) {
                process.exit(1);
            }
            self.mn_parent.finiAdm();
        });
    });
};

MantaAdmZk.prototype.do_list.help =
    'List configured ZooKeeper servers.\n\n' +
    'Usage:\n\n' +
    '    manta-adm zk list OPTIONS\n\n' +
    'Examples:\n\n' +
    '    # list ZooKeeper servers\n' +
    '    manta-adm zk list\n\n' +
    '    # list only IPs of ZK servers\n' +
    '    manta-adm zk list --omit-header -o ip\n\n' +
    '{{options}}\n' +
    'Available columns for -o:\n    ' +
    madm.zkColumnNames().join(', ');

/*
 * Note that the "manta-adm" commands that may modify the system use
 * /var/log/manta-adm.log as the default log file, as those logs currently serve
 * as general debug logs.  But the "zk list" subcommand is read-only and only
 * applicable to this user, so we use a path in /var/tmp for the log.
 */
MantaAdmZk.prototype.do_list.options = [
    maCommonOptions.omitHeader,
    maCommonOptions.logFileDefaultNone,
    maCommonOptions.columns
];

MantaAdmZk.prototype.do_fixup = function(_subcmd, opts, args, callback) {
    var self = this;
    var adm, nissues, nfixed;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    vasync.pipeline(
        {
            funcs: [
                function initAdm(_, stepcb) {
                    self.mn_parent.initAdm(opts, function() {
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
                        problems.critical.forEach(function(e) {
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

                    console.error('The following issues should be repaired:');
                    problems.fixable.forEach(function(e) {
                        console.error('error: %s', e.message);
                    });

                    if (opts.dryrun) {
                        console.error('To repair, leave off -n (--dry-run)');
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
                        function(proceed) {
                            process.stdout.write('\n');
                            if (!proceed) {
                                stepcb(new Error('aborted by user'));
                            } else {
                                stepcb();
                            }
                        }
                    );
                },
                function repair(_, stepcb) {
                    if (opts.dryrun || nissues === 0) {
                        stepcb();
                        return;
                    }

                    adm.fixupZkServers(function(err, n) {
                        if (!err) {
                            nfixed = n;
                        }
                        stepcb(err);
                    });
                }
            ]
        },
        function(err) {
            if (err) {
                fatal(err.message);
            }
            if (!opts.dryrun && nissues > 0) {
                console.error(
                    '%d issue%s repaired',
                    nfixed,
                    nfixed === 1 ? '' : 's'
                );
            }
            self.mn_parent.finiAdm();
            callback();
        }
    );
};

MantaAdmZk.prototype.do_fixup.help = [
    'Repair ZooKeeper configuration.',
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

MantaAdmZk.prototype.do_fixup.options = [
    maCommonOptions.confirm,
    maCommonOptions.dryrun,
    maCommonOptions.logFile
];

function MantaAdmAlarm(parent) {
    this.maa_parent = parent;
    cmdln.Cmdln.call(this, {
        name: parent.name + ' alarm',
        desc: 'View and configure information about alarms.'
    });
}

util.inherits(MantaAdmAlarm, cmdln.Cmdln);

/*
 * Performs common initialization steps used for the "manta-adm alarm"
 * subcommands.  Named arguments:
 *
 *     sources      Describes which data to load.  See alarmsInit in lib/adm.js.
 *
 *     clioptions   Parsed CLI options, as provided by node-cmdln.  This
 *                  function processes the "concurrency" and "config_file"
 *                  options, plus the options processed by initAdm().
 *
 *     skipWarnings By default, warnings encountered while fetching alarm
 *                  configuration are printed out.  If this option is true, then
 *                  these warnings are ignored.
 *
 *     skipFetch    By default, Triton objects (VMs, CNs, and SAPI information)
 *                  are fetched.  This takes some time, but this information is
 *                  needed by most subcommands.  If this option is true, then
 *                  this step is skipped.
 */
MantaAdmAlarm.prototype.initAdmAndFetchAlarms = function(args, callback) {
    var self = this;
    var clioptions, skipWarnings, initArgs, funcs;

    assertplus.object(args, 'args');
    assertplus.object(args.sources, 'args.sources');
    assertplus.object(args.clioptions, 'clioptions');
    assertplus.optionalBool(args.skipWarnings, 'args.skipWarnings');
    assertplus.optionalBool(args.skipFetch, 'args.skipFetch');

    skipWarnings = args.skipWarnings;
    clioptions = args.clioptions;
    initArgs = {
        configFile: clioptions.config_file,
        concurrency: clioptions.concurrency || maDefaultAlarmConcurrency,
        sources: args.sources
    };

    funcs = [];
    funcs.push(function initAdm(_, stepcb) {
        self.maa_parent.initAdm(clioptions, stepcb);
    });

    if (!args.skipFetch) {
        funcs.push(function fetch(_, stepcb) {
            self.maa_parent.madm_adm.fetchDeployed(stepcb);
        });
    }

    funcs.push(function fetchAmon(_, stepcb) {
        self.maa_parent.madm_adm.alarmsInit(initArgs, stepcb);
    });

    vasync.pipeline(
        {
            funcs: funcs
        },
        function(err) {
            var errors;

            if (err) {
                fatal(err.message);
            }

            if (!skipWarnings) {
                errors = self.maa_parent.madm_adm.alarmWarnings();
                errors.forEach(function(e) {
                    cmdutil.warn(e);
                });
            }

            callback();
        }
    );
};

MantaAdmAlarm.prototype.do_close = function(_subcmd, opts, args, callback) {
    var parent;

    if (args.length < 1) {
        callback(new Error('expected ALARMID'));
        return;
    }

    parent = this.maa_parent;
    this.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: {}
        },
        function() {
            var adm = parent.madm_adm;
            adm.alarmsClose(
                {
                    alarmIds: args,
                    concurrency: opts.concurrency
                },
                function(err) {
                    if (err) {
                        VError.errorForEach(err, function(e) {
                            console.error('error: %s', e.message);
                        });

                        process.exit(1);
                    }

                    parent.finiAdm();
                    callback();
                }
            );
        }
    );
};

MantaAdmAlarm.prototype.do_close.help = [
    'Close open alarms.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm close ALARMID...',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarm.prototype.do_close.options = [
    maCommonOptions.concurrency,
    maCommonOptions.configFile
];

MantaAdmAlarm.prototype.do_config = MantaAdmAlarmConfig;

MantaAdmAlarm.prototype.do_details = function(_subcmd, opts, args, callback) {
    this.doAlarmPrintSubcommand(opts, 1, args, callback);
};

MantaAdmAlarm.prototype.do_details.help = [
    'Print details about an alarm.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm details ALARMID...',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarm.prototype.do_details.options = [maCommonOptions.configFile];

MantaAdmAlarm.prototype.do_faults = function(_subcmd, opts, args, callback) {
    this.doAlarmPrintSubcommand(opts, undefined, args, callback);
};

MantaAdmAlarm.prototype.do_faults.help = [
    "Print information about all of an alarm's faults.",
    '',
    'Usage:',
    '',
    '    manta-adm alarm faults ALARMID...',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarm.prototype.do_faults.options = [maCommonOptions.configFile];

MantaAdmAlarm.prototype.doAlarmPrintSubcommand = function doAlarmPrintSubcommand(
    opts,
    nmaxfaults,
    args,
    callback
) {
    var self = this;
    var sources = {};

    if (args.length < 1) {
        callback(new Error('expected ALARMID'));
        return;
    }

    sources = {
        configBasic: true,
        alarms: {
            alarmIds: args
        }
    };

    this.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources,
            skipWarnings: true
        },
        function() {
            var nerrors = 0;
            args.forEach(function(id) {
                var error;

                error = self.maa_parent.madm_adm.alarmPrint({
                    id: id,
                    stream: process.stdout,
                    nmaxfaults: nmaxfaults
                });

                if (error instanceof Error) {
                    cmdutil.warn(error);
                    nerrors++;
                }

                console.log('');
            });

            if (nerrors > 0) {
                process.exit(1);
            }

            self.maa_parent.finiAdm();
        }
    );
};

MantaAdmAlarm.prototype.do_list = function(_subcmd, opts, args, callback) {
    var self = this;
    var options = {};
    var sources = {};

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    switch (opts.state) {
        case 'all':
        case 'closed':
        case 'open':
        case 'recent':
            break;

        default:
            callback(new VError('unsupported state: %s', opts.state));
            return;
    }

    options = listPrepareArgs(opts, madm.alarmColumnNames());
    if (options instanceof Error) {
        callback(options);
        return;
    }

    sources = {
        configBasic: true,
        alarms: {
            state: opts.state
        }
    };

    options.stream = process.stdout;
    this.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources
        },
        function() {
            self.maa_parent.madm_adm.alarmsList(options);
            self.maa_parent.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarm.prototype.do_list.help = [
    'List open alarms.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm list OPTIONS',
    '',
    '{{options}}',
    '',
    'Available columns for -o:\n    ' + madm.alarmColumnNames().join(', ')
].join('\n');

MantaAdmAlarm.prototype.do_list.options = [
    maCommonOptions.configFile,
    maCommonOptions.omitHeader,
    maCommonOptions.columns,
    {
        names: ['state'],
        type: 'string',
        help: 'List only alarms in specified state',
        default: 'open'
    }
];

MantaAdmAlarm.prototype.do_maint = MantaAdmAlarmMaint;

MantaAdmAlarm.prototype.do_metadata = MantaAdmAlarmMetadata;

MantaAdmAlarm.prototype.do_notify = function(_subcmd, opts, args, callback) {
    var parent;
    var allowedArg0 = {
        enabled: true,
        enable: true,
        on: true,
        true: true,
        yes: true,

        disabled: false,
        disable: false,
        off: false,
        false: false,
        no: false
    };

    if (args.length < 2) {
        callback(new Error('expected arguments'));
        return;
    }

    if (!allowedArg0.hasOwnProperty(args[0])) {
        callback(new Error('expected "on" or "off"'));
        return;
    }

    parent = this.maa_parent;
    this.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: {}
        },
        function() {
            var adm = parent.madm_adm;
            adm.alarmsUpdateNotification(
                {
                    alarmIds: args.slice(1),
                    concurrency: opts.concurrency,
                    suppressed: !allowedArg0[args[0]]
                },
                function(err) {
                    if (err) {
                        VError.errorForEach(err, function(e) {
                            console.error('error: %s', e.message);
                        });

                        process.exit(1);
                    }

                    parent.finiAdm();
                    callback();
                }
            );
        }
    );
};

MantaAdmAlarm.prototype.do_notify.help = [
    'Enable or disable alarm notifications.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm notify on|off ALARMID...',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarm.prototype.do_notify.options = [
    maCommonOptions.concurrency,
    maCommonOptions.configFile
];

MantaAdmAlarm.prototype.do_show = function(_subcmd, opts, args, callback) {
    var parent, sources;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    parent = this.maa_parent;
    sources = {
        configBasic: true,
        alarms: {
            state: 'open'
        }
    };

    this.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources
        },
        function() {
            var showArgs = {stream: process.stdout};
            parent.madm_adm.alarmsShow(showArgs);
            parent.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarm.prototype.do_show.help = [
    'Summarize open alarms.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm show',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarm.prototype.do_show.options = [maCommonOptions.configFile];

function MantaAdmAlarmConfig(parent) {
    this.maac_parent = parent;
    this.maac_root = parent.maa_parent;

    cmdln.Cmdln.call(this, {
        name: parent.name + ' config',
        desc: 'Manage probe and probe group configuration.'
    });
}

util.inherits(MantaAdmAlarmConfig, cmdln.Cmdln);

MantaAdmAlarmConfig.prototype.do_probegroup = MantaAdmAlarmProbeGroup;

MantaAdmAlarmConfig.prototype.do_show = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var root, parent, adm, sources;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    root = this.maac_root;
    parent = this.maac_parent;
    sources = {
        configFull: true
    };

    parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources
        },
        function() {
            adm = root.madm_adm;
            adm.alarmConfigShow({
                stream: process.stdout
            });

            root.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarmConfig.prototype.do_show.help = [
    'Summarize configured probes and probe groups.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm config show',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmConfig.prototype.do_show.options = [
    maCommonOptions.concurrency,
    maCommonOptions.configFile
];

MantaAdmAlarmConfig.prototype.do_update = function(
    _subcmd,
    opts,
    args,
    callback
) {
    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    this.amonUpdateSubcommand(opts, opts.dryrun, callback);
};

MantaAdmAlarmConfig.prototype.do_update.help = [
    'Update and probes and probe groups that are out of date.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm config update OPTIONS',
    '    manta-adm alarm config update OPTIONS --unconfigure',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmConfig.prototype.do_update.options = [
    maCommonOptions.confirm,
    maCommonOptions.concurrency,
    maCommonOptions.configFile,
    maCommonOptions.dryrun,
    maCommonOptions.unconfigure
];

MantaAdmAlarmConfig.prototype.do_verify = function(
    _subcmd,
    opts,
    args,
    callback
) {
    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    this.amonUpdateSubcommand(opts, true, callback);
};

MantaAdmAlarmConfig.prototype.do_verify.help = [
    'Check that deployed probes and probe groups are up to date.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm config verify OPTIONS',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmConfig.prototype.do_verify.options = [
    maCommonOptions.concurrency,
    maCommonOptions.configFile,
    maCommonOptions.unconfigure
];

MantaAdmAlarmConfig.prototype.amonUpdateSubcommand = function(
    clioptions,
    dryrun,
    callback
) {
    var self = this;
    var root, parent, sources, adm, plan;

    assertplus.object(clioptions, 'clioptions');
    assertplus.number(clioptions.concurrency, 'clioptions.concurrency');
    assertplus.bool(clioptions.unconfigure, 'clioptions.unconfigure');

    root = self.maac_root;
    parent = self.maac_parent;
    sources = {
        configFull: true
    };
    vasync.pipeline(
        {
            arg: null,
            funcs: [
                function init(_, stepcb) {
                    parent.initAdmAndFetchAlarms(
                        {
                            clioptions: clioptions,
                            sources: sources
                        },
                        stepcb
                    );
                },
                function generateAmonPlan(_, stepcb) {
                    var options;

                    adm = root.madm_adm;
                    options = {
                        unconfigure: clioptions.unconfigure
                    };
                    plan = adm.amonUpdatePlanCreate(options);
                    if (plan instanceof Error) {
                        stepcb(plan);
                        return;
                    }

                    adm.amonUpdatePlanDump({
                        plan: plan,
                        stream: process.stderr,
                        verbose: false
                    });

                    if (!plan.needsChanges()) {
                        console.log('no changes to make');
                        stepcb();
                        return;
                    }

                    if (dryrun) {
                        console.log(
                            'To apply these changes, ' +
                                'use the "update" subcommand without ' +
                                'the -n/--dry-run option.'
                        );
                        stepcb();
                        return;
                    }

                    if (clioptions.confirm) {
                        stepcb();
                        return;
                    }

                    common.confirm(
                        'Are you sure you want to proceed? (y/N): ',
                        function(proceed) {
                            if (!proceed) {
                                stepcb(new Error('aborted by user'));
                            } else {
                                stepcb();
                            }
                        }
                    );
                },
                function execAmonPlan(_, stepcb) {
                    if (dryrun || !plan.needsChanges()) {
                        stepcb();
                        return;
                    }

                    adm.amonUpdatePlanApply(
                        {
                            concurrency: clioptions.concurrency,
                            plan: plan,
                            stream: process.stderr
                        },
                        stepcb
                    );
                }
            ]
        },
        function(err) {
            root.finiAdm();
            callback(err);
        }
    );
};

/*
 * Maintenance windows
 */

function MantaAdmAlarmMaint(parent) {
    this.mam_parent = parent;
    this.mam_root = parent.maa_parent;

    cmdln.Cmdln.call(this, {
        name: parent.name + ' maint',
        desc: 'View and manage maintenance windows.'
    });
}

util.inherits(MantaAdmAlarmMaint, cmdln.Cmdln);

MantaAdmAlarmMaint.prototype.do_create = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var nscopes, scopeProp, targets;
    var params, i;
    var tstart, tend, tnow;
    var parent, root;

    /*
     * We accept no non-option arguments.
     */
    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    /*
     * Parse and validate the scope options.
     */
    nscopes = 0;
    targets = [];
    if (opts.probe) {
        nscopes++;
        targets = opts.probe.slice(0);
        scopeProp = 'probes';
    }
    if (opts.probegroup) {
        nscopes++;
        targets = opts.probegroup.slice(0);
        scopeProp = 'probeGroups';
    }
    if (opts.machine) {
        nscopes++;
        targets = opts.machine.slice(0);
        scopeProp = 'machines';
    }

    if (nscopes > 1) {
        callback(
            new VError(
                'only one of --probe, --probegroup, or ' +
                    '--machine may be specified'
            )
        );
        return;
    }

    /*
     * We cannot easily validate these identifiers against the set of
     * deployed probes, probe groups, or machines, but we can detect some
     * cases of obviously bogus input.
     */
    params = {};
    if (nscopes > 0) {
        for (i = 0; i < targets.length; i++) {
            if (!/^[a-zA-Z0-9_-]+$/.test(targets[i])) {
                callback(
                    new VError(
                        'identifier "%s": does not look like a valid uuid',
                        targets[i]
                    )
                );
                return;
            }
        }

        params[scopeProp] = targets;
    } else {
        params['all'] = true;
    }

    tnow = Date.now();

    /*
     * The "--start" and "--end" options are required.
     *
     * "--start" may have the special value "now", in which case we'll
     * generate a start timestamp based on the current time.  That means we
     * have to parse it here and not rely on dashdash's "date" type.
     */
    if (!opts.start) {
        callback(new VError('argument is required: --start'));
        return;
    }
    if (opts.start === 'now') {
        params['start'] = new Date(tnow);
    } else {
        var d = Date.parse(opts.start);
        if (isNaN(d)) {
            callback(
                new VError('unsupported value for --start: %s', opts.start)
            );
            return;
        }

        params['start'] = new Date(d);
    }

    if (!opts.end) {
        callback(new VError('argument is required: --end'));
        return;
    }
    params['end'] = opts.end;

    /*
     * --notes is required unless the user specifies the undocumented
     * --no-notes option.  The rationale is that we want to require
     * operators to provide some kind of note, but it's useful in
     * development to have a tool for creating a window with no note so that
     * we can test the CLI's behavior when encountering such windows.
     */
    if (!opts.notes && !opts.no_notes) {
        callback(new VError('argument is required: --notes'));
        return;
    }
    if (opts.notes) {
        params['notes'] = opts.notes;
    }

    /*
     * Validate the semantics of the time window.
     */
    tstart = params['start'].getTime();
    assertplus.number(tstart);
    assertplus.ok(!isNaN(tstart));

    tend = params['end'].getTime();
    assertplus.number(tend);
    assertplus.ok(!isNaN(tend));

    if (tend <= tstart) {
        callback(new VError('specified window does not start before it ends'));
        return;
    }

    if (tend < tnow) {
        callback(new VError('cannot create windows in the past'));
        return;
    }

    console.log(
        'creating maintenance window of duration %s:',
        common.fmtDuration(tend - tstart)
    );
    console.log('    from %s', params['start'].toISOString());
    console.log('    to   %s', params['end'].toISOString());

    if (tstart < tnow) {
        console.error('note: maintenance window starts in the past');
    }

    if (tend - tstart > maMaintWindowLong) {
        console.error(
            'note: maintenance window exceeds expected maximum (%s)',
            common.fmtDuration(maMaintWindowLong)
        );
    }

    root = this.mam_root;
    parent = this.mam_parent;
    parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: {}
        },
        function() {
            var adm = root.madm_adm;
            adm.alarmsMaintWindowCreate(
                {
                    windef: params
                },
                function(err, maintwin) {
                    if (err) {
                        fatal(err.message);
                    }

                    console.log('window created: %d', maintwin.win_id);
                    root.finiAdm();
                    callback();
                }
            );
        }
    );
};

MantaAdmAlarmMaint.prototype.do_create.help = [
    'Create (schedule) a future maintenance window.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm maint create OPTIONS',
    '',
    'The --start, --end, and --notes options are required.  See the manual',
    'page for details.',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmMaint.prototype.do_create.options = [
    maCommonOptions.configFile,
    {
        names: ['start'],
        type: 'string',
        help: 'Start time of the window (use "now" to start immediately)'
    },
    {
        names: ['end'],
        type: 'date',
        help: 'End time of the window'
    },
    {
        names: ['notes'],
        type: 'string',
        help: 'Notes (typically use a JIRA ticket number)'
    },
    {
        names: ['no-notes'],
        type: 'bool',
        hidden: true,
        help: 'Omit "notes" field (for dev only)'
    },
    {
        names: ['probe'],
        type: 'arrayOfString',
        helpArg: 'PROBEID...',
        help: 'List of probes affected by window (default: all)'
    },
    {
        names: ['probegroup'],
        type: 'arrayOfString',
        helpArg: 'GROUPID...',
        help: 'List of probe groups affected by window (default: all)'
    },
    {
        names: ['machine'],
        type: 'arrayOfString',
        helpArg: 'MACHINEID...',
        help: 'List of machines affected by window (default: all)'
    }
];

MantaAdmAlarmMaint.prototype.do_delete = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var parent, root;

    if (args.length < 1) {
        callback(new Error('expected WINID'));
        return;
    }

    root = this.mam_root;
    parent = this.mam_parent;
    parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: {}
        },
        function() {
            var adm = root.madm_adm;
            adm.alarmsMaintWindowsDelete(
                {
                    winIds: args,
                    concurrency: opts.concurrency
                },
                function(err) {
                    if (err) {
                        VError.errorForEach(err, function(e) {
                            console.error('error: %s', e.message);
                        });

                        process.exit(1);
                    }

                    root.finiAdm();
                    callback();
                }
            );
        }
    );
};

MantaAdmAlarmMaint.prototype.do_delete.help = [
    'Delete (cancel) pending maintenance windows.',
    '',
    'Usage:',
    '',
    '    manta-adm alarm maint delete WINID...',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmMaint.prototype.do_delete.options = [
    maCommonOptions.concurrency,
    maCommonOptions.configFile
];

MantaAdmAlarmMaint.prototype.do_list = function cmdMaintList(
    _subcmd,
    opts,
    args,
    callback
) {
    var self = this;
    var options = {};
    var sources;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    options = listPrepareArgs(opts, madm.maintWindowColumnNames());
    if (options instanceof Error) {
        callback(options);
        return;
    }

    options.stream = process.stdout;
    sources = {windows: true};
    this.mam_parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources
        },
        function() {
            self.mam_root.madm_adm.alarmsMaintWindowsList(options);
            self.mam_root.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarmMaint.prototype.do_list.help = [
    'List maintenance windows',
    '',
    'Usage:',
    '',
    '    manta-adm alarm maint list OPTIONS',
    '',
    '{{options}}',
    '',
    'Available columns for -o:\n',
    '    ' + madm.maintWindowColumnNames().join(', ')
].join('\n');

MantaAdmAlarmMaint.prototype.do_list.options = [
    maCommonOptions.omitHeader,
    maCommonOptions.columns,
    maCommonOptions.configFile
];

MantaAdmAlarmMaint.prototype.do_show = function cmdMaintShow(
    _subcmd,
    opts,
    args,
    callback
) {
    var self = this;
    var options = {};
    var sources;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    options.stream = process.stdout;
    sources = {windows: true};
    this.mam_parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources
        },
        function() {
            self.mam_root.madm_adm.alarmsMaintWindowsShow(options);
            self.mam_root.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarmMaint.prototype.do_show.help = [
    'Show details about maintenance windows',
    '',
    'Usage:',
    '',
    '    manta-adm alarm maint show OPTIONS',
    '',
    '{{options}}',
    '',
    'Available columns for -o:\n',
    '    ' + madm.maintWindowColumnNames().join(', ')
].join('\n');

MantaAdmAlarmMaint.prototype.do_show.options = [maCommonOptions.configFile];

/*
 * Local alarm metadata
 */

function MantaAdmAlarmMetadata(parent) {
    this.maam_parent = parent;
    this.maam_root = parent.maa_parent;

    cmdln.Cmdln.call(this, {
        name: parent.name + ' metadata',
        desc: 'View local metadata about alarm config.'
    });
}

util.inherits(MantaAdmAlarmMetadata, cmdln.Cmdln);

MantaAdmAlarmMetadata.prototype.do_events = function cmdEvents(
    _subcmd,
    opts,
    args,
    callback
) {
    var self = this;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    this.maam_parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: {},
            skipFetch: true
        },
        function() {
            var events = self.maam_root.madm_adm.alarmEventNames();
            events.forEach(function(eventName) {
                console.log(eventName);
            });
            self.maam_root.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarmMetadata.prototype.do_events.help = [
    'List known event names.',
    '',
    'Usage:',
    '    {{name}} events',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmMetadata.prototype.do_events.options = [
    maCommonOptions.configFile
];

MantaAdmAlarmMetadata.prototype.do_ka = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var self = this;

    this.maam_parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: {},
            skipFetch: true
        },
        function() {
            var events, nerrors;
            var root = self.maam_root;

            if (args.length === 0) {
                events = root.madm_adm.alarmEventNames();
            } else {
                events = args;
            }

            nerrors = 0;
            events.forEach(function(eventName) {
                var error;
                error = root.madm_adm.alarmKaPrint({
                    eventName: eventName,
                    stream: process.stdout
                });

                if (error instanceof Error) {
                    cmdutil.warn(error);
                    nerrors++;
                }

                console.log('');
            });

            if (nerrors > 0) {
                process.exit(1);
            }
            root.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarmMetadata.prototype.do_ka.help = [
    'Print information about events.',
    '',
    'Usage:',
    '    {{name}} ka [EVENT_NAME]',
    '',
    '{{options}}'
].join('\n');

MantaAdmAlarmMetadata.prototype.do_ka.options = [maCommonOptions.configFile];

function MantaAdmAlarmProbeGroup(parent) {
    this.maap_parent = parent;
    this.maap_root = parent.maac_root;

    cmdln.Cmdln.call(this, {
        name: parent.name + ' probegroup',
        desc: 'View and configure information about amon probe groups.'
    });
}

util.inherits(MantaAdmAlarmProbeGroup, cmdln.Cmdln);

MantaAdmAlarmProbeGroup.prototype.do_list = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var self = this;
    var options = {};
    var sources;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }

    options = listPrepareArgs(opts, madm.probeGroupColumnNames());
    if (options instanceof Error) {
        callback(options);
        return;
    }

    /*
     * We fetch the list of open alarms in order to count the alarms for
     * each probe group.
     */
    sources = {
        configFull: true,
        alarms: {
            state: 'open'
        }
    };

    options.stream = process.stdout;
    this.maap_parent.maac_parent.initAdmAndFetchAlarms(
        {
            clioptions: opts,
            sources: sources
        },
        function() {
            self.maap_root.madm_adm.alarmsProbeGroupsList(options);
            self.maap_root.finiAdm();
            callback();
        }
    );
};

MantaAdmAlarmProbeGroup.prototype.do_list.help = [
    'List open alarms',
    '',
    'Usage:',
    '    {{name}} list OPTIONS',
    '',
    '{{options}}',
    'Available columns for -o:',
    '    ' + madm.probeGroupColumnNames().join(', ')
].join('\n');

MantaAdmAlarmProbeGroup.prototype.do_list.options = [
    maCommonOptions.omitHeader,
    maCommonOptions.columns,
    maCommonOptions.configFile
];

MantaAdm.prototype.do_create_topology = function(
    _subcmd,
    opts,
    args,
    callback
) {
    var self = this;

    if (args.length > 0) {
        callback(new Error('unexpected arguments'));
        return;
    }
    if (!opts.t) {
        callback(new VError('argument is required: -t'));
        return;
    }
    if (opts.t !== 'directory' && opts.t !== 'buckets') {
        callback(
            new VError(
                'unsupported value for -t: %s. Valid ' +
                    'values are "directory" and "buckets".',
                opts.t
            )
        );
        return;
    }
    if (!opts.v) {
        callback(new VError('argument is required: -v'));
        return;
    }
    if (!opts.p) {
        callback(new VError('argument is required: -p'));
        return;
    }
    var force = opts.f === true;

    self.initAdm(opts, function initCb() {
        var adm = self.madm_adm;
        adm.createTopology(
            {
                buckets: opts.t === 'buckets',
                vnodes: opts.v,
                port: opts.p,
                force: force
            },
            function createdTopology(err) {
                self.finiAdm();
                callback(err);
            }
        );
    });
};

MantaAdm.prototype.do_create_topology.helpOpts = {
    maxHelpCol: 23
};

MantaAdm.prototype.do_create_topology.help = [
    'Creates a consistent hash ring used by electric-moray.',
    '',
    'Usage:',
    '  manta-adm create-topology -t RING_TYPE -v VNODES -p PORT',
    '',
    '{{options}}',
    'The ring is created and uploaded to imgapi. The resulting image UUID',
    'is persisted in SAPI on the Manta application as',
    'metadata.HASH_RING_IMAGE',
    '',
    'WARNING: Run this command with care. Improper use such as generating ',
    'a bad ring or a different ring in production will result in the ',
    'corruption of Manta metadata.'
].join('\n');

MantaAdm.prototype.do_create_topology.options = [
    maCommonOptions.logFile,
    {
        names: ['t'],
        type: 'string',
        default: 'directory',
        helpArg: 'RING_TYPE',
        help:
            'Type of ring to create. Valid values are "directory" and "buckets"'
    },
    {
        names: ['v'],
        type: 'integer',
        helpArg: 'VNODES',
        help: 'Number of vnodes to include in the ring'
    },
    {
        names: ['p'],
        type: 'integer',
        helpArg: 'PORT',
        help:
            'Port of moray/boray instances that electric-moray will connect to'
    },
    {
        names: ['f'],
        type: 'bool',
        help:
            'force generation of new hash ring even if one already exists in ' +
            'SAPI metadata'
    }
];

/*
 * Named arguments:
 *
 *     opts		options provided by cmdln
 *
 *     allowed		allowed column names
 *
 * Returns either an Error describing invalid command-line arguments or an
 * object with "columns" and "omitHeader" set according to the options.
 */
function listPrepareArgs(opts, allowed) {
    var options, selected;

    options = {};
    if (opts.columns) {
        selected = checkColumns(allowed, opts.columns);
        if (selected instanceof Error) {
            return selected;
        }

        options.columns = selected;
    }

    if (opts.omit_header) {
        options.omitHeader = true;
    } else {
        options.omitHeader = false;
    }

    return options;
}

function checkColumns(allowed, columns) {
    var selected, c, i;

    selected = [];
    for (i = 0; i < columns.length; i++) {
        c = columns[i].split(',');
        selected = selected.concat(columns[i].split(','));
    }

    for (i = 0; i < selected.length; i++) {
        c = selected[i];
        if (allowed.indexOf(c) === -1) {
            return new VError('unknown column: "%s"', c);
        }
    }

    return selected;
}

function fatal(msg) {
    console.error('%s: %s', maArg0, msg);
    process.exit(1);
}

cmdutil.exitOnEpipe();
cmdln.main(MantaAdm);
