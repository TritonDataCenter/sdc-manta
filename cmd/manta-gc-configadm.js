#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * manta-gc-configadm.js: CLI tool for special operations on garbage-collector
 * configuration. This tool makes potentially-breaking changes to state that
 * `manta-adm accel-gc` depends on.
 *
 * Most use-cases do not require this tool and should instead be addressed with
 * one of the `manta-adm accel-gc` subcommands.
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var cmdln = require('cmdln');
var cmdutil = require('cmdutil');
var fprintf = require('extsprintf').fprintf;
var fs = require('fs');
var jsprim = require('jsprim');
var path = require('path');
var restifyClients = require('restify-clients');
var util = require('util');
var vasync = require('vasync');

var common = require('../lib/common');
var deploy = require('../lib/deploy');
var madm = require('../lib/adm');

var VError = require('verror').VError;
var MultiError = require('verror').MultiError;

var maGcCfgArg0 = path.basename(process.argv[1]);

var maGcCfgCommonOptions = {
    'confirm': {
	'names': [ 'confirm', 'y' ],
	'type': 'bool',
	'help': 'Bypass all confirmations (be careful!)'
    }
};

function MantaGcConfigAdm()
{
	cmdln.Cmdln.call(this, {
	    name: 'manta-gc-configadm',
	    desc: 'Tool for special operations involving ' +
		'garbage-collector configuration.\n\nMost use-' +
		'cases do not require this tool. Make sure ' +
		'the desired operation\nis not already ' +
		'implemented by `manta-adm accel-gc`.',
	    options: [
		{names: ['help', 'h'], type: 'bool',
		    help: 'Print help and exit.'},
		{name: 'version', type: 'bool',
		    help: 'Print version and exit.'}
	    ]
	});
}
util.inherits(MantaGcConfigAdm, cmdln.Cmdln);

/*
 * Performs common initialization steps used by most subcommands.  "opts" are
 * the cmdln-parsed CLI options.  This function processes the "log_file" option.
 */
MantaGcConfigAdm.prototype.initAdm = function (opts, callback)
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
		    'level': process.env['LOG_LEVEL'] || 'fatal',
		    'stream': process.stderr
		} ];
	}

	this.madm_log = new bunyan({
	    'name': maGcCfgArg0,
	    'streams': logstreams,
	    'serializers': restifyClients.bunyan.serializers
	});

	this.madm_adm = new madm.MantaAdm(this.madm_log);
	this.madm_adm.loadSdcConfig(function (err) {
		if (err)
			fatal(err.message);
		callback();
	});
};

MantaGcConfigAdm.prototype.finiAdm = function ()
{
	this.madm_adm.close();
};

/*
 * Some Manta deployments may have garbage-collectors that expect a deprecated
 * set of SAPI variables. To ease the operational load of migrating the
 * collectors to use the current set of variables, the following subcommand does
 * the following:
 *
 * 1. Retrieves the set of instance-level metadata for all garbage-collectors
 * 2. Validates that all of the garbage-collectors use the same set of SAPI
 *    variables for configuration
 * 3. Translates each instance-level garbage-collector configuration from the
 *    old set of variables to the new set
 * 4. Commits the translated configuration to SAPI
 * 5. Updates the SAPI garbage-collector service configuration with default
 *    values for the new variables
 *
 * By default, this subcommand will not remove any of the old configuration
 * variables from either the service configuration object, or the instance-level
 * configuration objects. Running in the default mode, this will configure
 * garbage-collection with the union of the old and new configuration variables.
 *
 * If the `--cleanup` option is used, this function will remove the
 * deprecated SAPI variables from both the instance-level and service-level
 * configuration objects.
 *
 * The operation of this mode is idemptotent in the sense that it may be invoked
 * on a Manta where all collectors are deployed with the new set of variables.
 * The result is a no-op.
 *
 * The recommended use case for this subcommand is to run it in the default mode
 * (without the `--cleanup` option) first. When this succeeds and the
 * garbage-collectors come up with the 'union' configuration, the `--cleanup`
 * option may be passed to get rid of the deprecated variables.
 */
MantaGcConfigAdm.prototype.do_migrate_config =
function (subcmd, opts, args, callback)
{
	var self = this;
	var adm, log;

	var oldFields = [
		'GC_SHARD_NUM_LO',
		'GC_SHARD_NUM_HI'
	];
	var oldServiceFields = [
		'GC_MANTA_FASTDELETE_QUEUE_CONCURRENCY'
	];
	var newFields = common.GC_METADATA_FIELDS;

	var changes = {
	    instances: {
		add: {},
		remove: {}
	    },
	    service: {
		add: {},
		remove: {}
	    }
	};

	var funcs = [
		function (next) {
			adm.fetchDeployed(function (err) {
				next(err);
			});
		},
		function (next) {
			adm.getDeployedInstanceMetadataJson({
				fields: oldFields.concat(newFields),
				svcname: 'garbage-collector'
			}, function (err, metadata) {
				log.debug({
					err: err,
					metadata: metadata
				}, 'got deployed garbage-collector ' +
				    'configs');
				next(err, metadata);
			});
		},
		function (metadata, next) {
			validateGcInstanceConfigs(metadata, oldFields,
			    newFields, function (err) {
				log.debug({
					err: err
				}, 'validated garbage-collector ' +
				    'configs');
				next(err, metadata);
			});
		},
		function (metadata, next) {
			translateGcInstanceConfigs(adm.ma_app, metadata,
			    function (err) {
				log.debug({
					err: err,
					metadata: metadata
				}, 'transformed garbage-collector ' +
				    'configs');
				next(err, metadata);
			});
		},
		function (metadata, next) {
			var remove = {};
			if (opts.cleanup) {
				function markFieldForRemove(uuid, field) {
					if (!remove.hasOwnProperty(uuid))
						remove[uuid] = {};
					if (!metadata[uuid] ||
					    !metadata[uuid].hasOwnProperty(
					    field))
						return;
					remove[uuid][field] =
					    metadata[uuid][field];
				}
				Object.keys(metadata).forEach(function (uuid) {
					oldFields.forEach(function (field) {
						markFieldForRemove(uuid, field);
					});
				});
			}
			changes.instances = {
			    add: metadata,
			    remove: remove
			};
			next();
		},
		function (next) {
			loadGcServiceMetadataUpdates(adm, next);
		},
		function (fileMetadata, next) {
			adm.getDeployedServiceMetadata({
			    svcname: 'garbage-collector',
			    fields: oldFields.concat(
				oldServiceFields)
			}, function (err, deployedMetadata) {
				next(err, jsprim.mergeObjects(
				    deployedMetadata, fileMetadata));
			});
		},
		function (metadata, next) {
			var removeMetadata = {};
			if (opts.cleanup) {
				function markFieldForRemove(field) {
					if (!metadata.hasOwnProperty(field))
						return;
					removeMetadata[field] =
					    metadata[field];
				}
				Object.keys(metadata).forEach(function (uuid) {
					oldFields.forEach(
					    markFieldForRemove);
					oldServiceFields.forEach(
					    markFieldForRemove);
				});
			}
			changes.service = {
			    add: metadata,
			    remove: removeMetadata
			};
			next();
		},
		function (next) {
			if (opts.confirm) {
				next();
				return;
			}
			dumpConfigMigrationChanges(changes, process.stdout);
			common.confirm('Apply migration? (y/N): ',
			    function (proceed) {
				process.stdout.write('\n');
				if (!proceed) {
					next(new Error('aborted by user'));
				} else {
					next();
				}
			});
		},
		function (next) {
			var instChanges = changes.instances;
			adm.updateDeployedInstanceMetadata({
			    svcname: 'garbage-collector'
			}, instChanges.add, instChanges.remove, function (err) {
				log.debug({
				    err: err,
				    add: instChanges.add,
				    remove: instChanges.remove
				}, 'updated SAPI instance(s) metadata');
				next(err);
			});
		},
		function (next) {
			var svcChanges = changes.service;
			adm.updateDeployedServiceMetadata({
			    svcname: 'garbage-collector'
			}, svcChanges.add, svcChanges.remove,
			    function (updateErr) {
				adm.ma_log.debug({
				    err: updateErr,
				    add: svcChanges.add,
				    remove: svcChanges.remove
				}, 'updated garbage-collector ' +
				    'service metadata');
				next(updateErr);
			});
		}
	];

	self.initAdm(opts, function () {
		adm = self.madm_adm;
		log = self.madm_log;

		vasync.waterfall(funcs, function (err) {
			if (err)
				fatal(err.message);
			self.finiAdm();
			callback();
		});
	});
};

MantaGcConfigAdm.prototype.do_migrate_config.help =
    'Migrate garbage-collector SAPI configuration in a deployment.\n\n' +
    '    manta-gc-configadm migrate-config [OPTIONS]\n\n'               +
    '{{options}}';

MantaGcConfigAdm.prototype.do_migrate_config.options = [
    maGcCfgCommonOptions.confirm,
    {
	'names': ['cleanup', 'c'],
	'type': 'bool',
	'help': 'Remove deprecated SAPI variables after migrating the ' +
	    'configuration. It is recommended that this option only '   +
	    'be used after migration without removal has succeeded.'
    }];

/*
 * Some Manta deployments may have garbage-collectors that expect a particular
 * set of deprecated SAPI variables. To reduce the operational load of
 * migrating to the new configuration, `manta-adm` has logic that validates that
 * the operator is migrating from a Manta in which all garbage-collectors are
 * deployed on the old set of variables to a Manta in which all
 * garbage-collectors are configured with the new set of variables.
 *
 * This function will report an error in the following cases:
 * - At least one garbage-collector is found to contain a partial old and
 *   partial new configuration. In this case, it is unclear which configuration
 *   to use for that collector.
 * - Some garbage-collectors in the Manta deployment are found to be on the old
 *   configuration, and some are found to be on the new configuration. This
 *   condition requires that the operator decide which configuration to use.
 *
 * The function succeeds all of the following are true:
 * - All collectors have either
 *   	a) A complete old configuration
 *   	b) A complete new configuration
 *   	c) A complete old and new configuration
 * - All collectors match the same case from the previous bullet
 *
 * Arguments:
 * - `metadata`: A JSON object mapping from garbage-collector instance uuid to
 *   SAPI instance metadata object.
 * - `oldFields`: An array of strings representing the 'deprecated' SAPI fields.
 * - `newFields`: An array of strings representing the new SAPI fields
 * - `callback`: A function that may be invoked with an error.
 */
function validateGcInstanceConfigs(metadata, oldFields, newFields, callback)
{
	var errors = [];
	Object.keys(metadata).forEach(function (uuid) {
		assertplus.uuid(uuid, 'expected garbage-collector ' +
		    'instance uuid');
		var md = metadata[uuid];
		var missingOld = oldFields.filter(
		    function (field) {
			return (!md.hasOwnProperty(field));
		});
		var missingNew = newFields.filter(function (field) {
			/*
			 * If `GC_CONCURRENCY` is not overridden for the
			 * instance, then it is set on the service.
			 */
			if (field === 'GC_CONCURRENCY')
				return (false);
			return (!md.hasOwnProperty(field));
		});
		if (missingOld.length != 0 &&
		    missingNew.length != 0) {
			errors.push(new VError('garbage-collector "%s" '  +
			    'has an incomplete instance config. Missing ' +
			    'old fields: %s. Missing new fields: %s',
			    uuid, missingOld.join(', '),
			    missingNew.join(', ')));
		}
	});

	if (errors.length > 0) {
		callback(new MultiError(errors));
		return;
	}
	callback();
}

/*
 * If `validateGcInstanceConfigs` succeeds, the following helper function may be
 * used to translate the instance metadata of all garbage-collectors in the
 * deployment from a version that uses the old fields to a version that uses the
 * new fields.
 *
 * This function will not remove any of the old fields. It will also not commit
 * any changes to the underlying SAPI data.
 *
 * Arguments:
 * - `app`: The Manta SAPI application object
 * - `metadata`: A JSON object mapping from garbage-collector instance uuid to
 *   SAPI instance metadata object.
 * - `callback`: A function that may be invoked with an error.
 */
function translateGcInstanceConfigs(app, metadata, callback)
{
	Object.keys(metadata).forEach(function (uuid) {
		assertplus.uuid(uuid, 'expected garbage-collector ' +
		    'instance uuid');
		var md = metadata[uuid];
		var assignedShards = [];
		var lo = md['GC_SHARD_NUM_LO'] || 0;
		var hi = md['GC_SHARD_NUM_HI'] || 0;

		var domain = app.metadata['DOMAIN_NAME'];

		if (!(lo === hi && lo === 0)) {
			for (var i = lo; i <= hi; i++) {
				assignedShards.push({
					host: [i, 'moray',
					    domain].join('.')
				});
			}
			assignedShards[
			    assignedShards.length - 1].last = true;
		}

		/*
		 * If we already have the target properties, then the migration
		 * has already been attempted. Don't overwrite it.
		 */
		md['GC_ASSIGNED_SHARDS'] = md['GC_ASSIGNED_SHARDS'] ||
		    assignedShards;

		/*
		 * `GC_MANTA_FASTDELETE_QUEUE_CONCURRENCY` may or may not be set
		 * on the instance-level object. If it is set, we need to copy
		 * it. If it's not set, then the property is inherited from the
		 * service.
		 */
		var oldConcurrency =
		    md['GC_MANTA_FASTDELETE_QUEUE_CONCURRENCY'];
    		if (oldConcurrency)
			md['GC_CONCURRENCY'] = md['GC_CONCURRENCY'] ||
			    oldConcurrency;
	});
	callback();
}

function loadGcServiceMetadataUpdates(adm, callback)
{
	var file = util.format('%s/../config/services/%s/service.json',
	    path.dirname(__filename), 'garbage-collector');
	var size = adm.ma_app.metadata['SIZE'];

	file = [file, size].join('.');

	fs.readFile(file, function (err, contents) {
		var addMetadata;
		if (err) {
			callback(err);
			return;
		}
		try {
			addMetadata = JSON.parse(
			    contents.toString('utf8')).metadata;
		} catch (e) {
			callback(new VError(e, 'parse "%s"', file));
			return;
		}

		callback(null, addMetadata);
	});
}

function dumpConfigMigrationChanges(changes, sout)
{
	function formatValue(val) {
		return (util.inspect(val));
	}

	var fields;
	var instAdd = changes.instances.add;
	var instRemove = changes.instances.remove;

	Object.keys(instAdd).forEach(function (uuid) {
		fprintf(sout, '\ngarbage-collector instance "%s"\n\n', uuid);
		fprintf(sout, ' maintain/add fields/values below: \n\n');
		fields = Object.keys(instAdd[uuid] || {});
		fields.forEach(function (field) {
			if (instAdd[uuid][field] === undefined)
				return;
			fprintf(sout, '\t%s : %s\n', field,
			    formatValue(instAdd[uuid][field]));
		});
		if (fields.length === 0)
			fprintf(sout, '\t(none)\n\n');

		fprintf(sout, '\n remove fields (if present) below:\n\n');
		fields = Object.keys(instRemove[uuid] || {});
		fields.forEach(function (field) {
			if (instRemove[uuid][field] === undefined)
				return;
			fprintf(sout, '\t%s : %s\n', field,
			    formatValue(instRemove[uuid][field]));
		});
		if (fields.length === 0)
			fprintf(sout, '\t(none)\n');
	});

	var svcAdd = changes.service.add;
	var svcRemove = changes.service.remove;

	fprintf(sout, '\ngarbage-collector service\n\n');

	fprintf(sout, ' maintain/add fields/values below:\n\n');
	fields = Object.keys(svcAdd);
	fields.forEach(function (field) {
		if (svcAdd[field] === undefined)
			return;
		fprintf(sout, '\t%s : %s\n', field,
		    formatValue(svcAdd[field]));
	});
	if (fields.length === 0)
		fprintf(sout, '\t(none)\n');

	fprintf(sout, '\n remove fields (if present) below:\n\n');
	fields = Object.keys(svcRemove);
	fields.forEach(function (field) {
		if (svcRemove[field] === undefined)
			return;
		fprintf(sout, '\t%s : %s\n', field,
		    formatValue(svcRemove[field]));
	});
	if (fields.length === 0)
		fprintf(sout, '\t(none)\n\n');
}

function fatal(msg)
{
	console.error('%s: %s', maGcCfgArg0, msg);
	process.exit(1);
}

cmdutil.exitOnEpipe();
cmdln.main(MantaGcConfigAdm);
