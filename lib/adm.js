/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/adm.js: library interface to the "manta-adm" functionality
 */

var assert = require('assert');
var assertplus = require('assert-plus');
var extsprintf = require('extsprintf');
var fs = require('fs');
var jsprim = require('jsprim');
var net = require('net');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;
var tab = require('tab');
var vasync = require('vasync');
var wordwrap = require('wordwrap');

var VError = require('verror').VError;
var MultiError = require('verror').MultiError;
var fprintf = extsprintf.fprintf;

var alarms = require('./alarms');
var common = require('../lib/common');
var deploy = require('../lib/deploy');
var layout = require('./layout');
var svcs = require('./services');
var instance_info = require('./instance_info');

/* Public interface (used only inside this module) */
exports.columnNames = columnNames;
exports.alarmColumnNames = alarmColumnNames;
exports.cnColumnNames = cnColumnNames;
exports.probeGroupColumnNames = probeGroupColumnNames;
exports.zkColumnNames = zkColumnNames;
exports.MantaAdm = maAdm;

var maMaxConcurrency = 50; /* concurrent requests to SDC services */

/*
 * ZooKeeper configuration property.  This can be overridden for testing.
 */
var maZkConfigProp = process.env['ZK_SERVERS_PROPNAME'] || 'ZK_SERVERS';

/*
 * Path to default alarm metadata.
 */
var maAlarmMetadataDirectory =
    path.join(__dirname, '..', 'alarm_metadata/probe_templates');

/*
 * Available output columns for the list of zones.
 */
var maColumns = {
    'datacenter': {
	'label': 'DATACENTER',
	'width': 10
    },
    'image': {
	'label': 'IMAGE',
	'width': 36
    },
    'primary_ip': {
	'label': 'PRIMARY IP',
	'width': 16
    },
    'service': {
	'label': 'SERVICE',
	'width': 16
    },
    'shard': {
	'label': 'SH',
	'width': 2,
	'align': 'right'
    },
    'storage_id': {
	'label': 'STORAGE ID',
	'width': 26
    },
    'zonename': {
	'label': 'ZONENAME',
	'width': 36
    },
    'zoneabbr': {
	'label': 'ZONEABBR',
	'width': 8
    },

    'gz_host': {
	'label': 'GZ HOST',
	'width': 17
    },
    'gz_admin_ip': {
	'label': 'GZ ADMIN IP',
	'width': 16
    },

    'count': {
	'label': 'COUNT',
	'width': 5,
	'align': 'right'
    },
    'indent': {
	'label': '',
	'width': 4
    },
    'version': {
	'label': 'VERSION',
	'width' : 42
    }
};

function columnNames()
{
	return (Object.keys(maColumns).filter(function (c) {
		return (maColumns[c]['label'] !== '');
	}));
}

var maAlarmColumns = {
    'alarm': {
	'label': 'ALARM',
	'width': 6
    },
    'dateopened': {
	'label': 'DATE_OPENED',
	'width': 10
    },
    'timeopened': {
	'label': 'TIME_OPENED',
	'width': 24
    },
    'dateclosed': {
	'label': 'DATE_CLOSED',
	'width': 10
    },
    'timeclosed': {
	'label': 'TIME_CLOSED',
	'width': 24
    },
    'datelast': {
	'label': 'DATE_LAST',
	'width': 10
    },
    'timelast': {
	'label': 'TIME_LAST',
	'width': 24
    },
    'nevents': {
	'label': 'NEVENTS',
	'width': 5,
	'align': 'right'
    },
    'nflts': {
	'label': 'NFLTS',
	'width': 5,
	'align': 'right'
    },
    'notify': {
	'label': 'NFY',
	'width': 3
    },
    'summary': {
	'label': 'SUMMARY',
	'width': 30
    }
};

function alarmColumnNames()
{
	return (Object.keys(maAlarmColumns));
}

var maProbeGroupColumns = {
    'uuid': {
	'label': 'UUID',
	'width': 36
    },
    'name': {
	'label': 'NAME',
	'width': 20
    },
    'contacts': {
	'label': 'CONTACTS',
	'width': 15
    },
    'enabled': {
	'label': 'ENAB',
	'width': 4,
	'align': 'right'
    },
    'nalarms': {
	'label': 'NALARMS',
	'width': 7,
	'align': 'right'
    },
    'nprobes': {
	'label': 'NPROBES',
	'width': 7,
	'align': 'right'
    }
};

function probeGroupColumnNames()
{
	return (Object.keys(maProbeGroupColumns));
}

var maCnColumns = {
    'server_uuid': {
	'label': 'SERVER UUID',
	'width': 36
    },
    'host': {
	'label': 'HOST',
	'width': 17
    },
    'dc': {
	'label': 'DC',
	'width': 9
    },
    'admin_ip': {
	'label': 'ADMIN IP',
	'width': 16
    },
    'ram': {
	'label': 'RAM',
	'align': 'right',
	'width': 6
    },
    'compute_id': {
	'label': 'COMPUTE ID',
	'width': 24
    },
    'storage_ids': {
	'label': 'STORAGE IDS',
	'width': 26
    },
    'kind': {
	'label': 'KIND',
	'width': 7
    }
};

function cnColumnNames()
{
	return (Object.keys(maCnColumns));
}


var maZkColumns = {
    'ord': {
	'label': '#',
	'width': 1
    },
    'datacenter': {
	'label': 'DATACENTER',
	'width': 10
    },
    'zoneabbr': {
	'label': 'ZONEABBR',
	'width': 8
    },
    'zonename': {
	'label': 'ZONENAME',
	'width': 36
    },
    'ip': {
	'label': 'IP',
	'width': 16
    },
    'port': {
	'label': 'PORT',
	'align': 'right',
	'width': 5
    }
};

function zkColumnNames()
{
	return (Object.keys(maZkColumns));
}

/*
 * Library interface for manta-adm functionality.  This object provides methods
 * for discovering deployed Manta zones and updating the deployment to match a
 * desired configuration.  The configuration is specified in JSON with the
 * following form:
 *
 *    {
 *        "$CN_UUID": {
 *            "$SERVICE_NAME": {
 *                "$IMAGE_UUID": $COUNT
 *            },
 *            ...
 *        }, ...
 *    }
 *
 * This configuration specifies for each compute node, for each service, for
 * each image, how many zones for that service using that image should exist on
 * that compute node.  The special CN_UUID "<any>" can be used to denote that a
 * group of instances should be present on any CNs in the current datacenter and
 * it doesn't matter what specific servers they're on.  This is mainly used in
 * development and testing.
 *
 * There are several supported use cases:
 *
 *     o "genconfig" operation: call loadSdcConfig, then fetchDeployed, then
 *       one of dumpConfigCoal, dumpConfigLab, or genconfigFromFile.
 *
 *     o "show" operation: call loadSdcConfig, then fetchDeployed, then one
 *       or more of the dumpDeployed* family of functions.
 *
 *     o "update" operation: call loadSdcConfig, then readConfigFromFile, then
 *       fetchDeployed, then generatePlan, then some combination of execPlan
 *       with "dryrun" options.
 *
 *     o ZK servers listing and auditing: call loadSdcConfig, then
 *       fetchDeployed, then some combination of dumpZkServers and
 *       fixupZkServers.
 *
 *     o alarm configuration: call loadSdcConfig, then likely fetchDeployed,
 *       then alarmsInit with an appropriate set of sources, then any of the
 *       alarm-related functions.
 *
 * Any other sequence of operations (skipping any of these, or duplicating those
 * the ones that can't explicitly be called more than once) is invalid.
 */
function maAdm(log)
{
	/* Configuration */
	this.ma_appname = 'manta';

	/* Helper objects */
	this.ma_log = log;	/* bunyan logger */
	this.ma_sdc = null;	/* handles for SDC clients (e.g., CNAPI) */

	/* Request tracking (for debugging) */
	this.ma_id = 0;
	this.ma_reqs = {};
	this.ma_recent = [];
	this.ma_recent_limit = 30;

	/* SAPI application object (e.g., sdc-sapi /applications/...) */
	this.ma_app = null;

	/*
	 * SAPI services objects, indexed by SAPI service uuid
	 * (e.g., sdc-sapi /services/$service_uuid)
	 */
	this.ma_services = null;

	/*
	 * SAPI instance objects (as an array), indexed by SAPI service uuid.
	 * this.ma_instances[service_uuid] is an array of the SAPI instances.
	 * (e.g., sdc-sapi /instances/$instance_uuid)
	 */
	this.ma_instances = null;

	/*
	 * All SAPI instances as a single, flattened array.  The values here are
	 * not the SAPI instance object (as with ma_instances), but rather an
	 * object with all-caps properties corresponding to the columns that we
	 * allow users to select.  This is useful for streaming to a node-tab
	 * stream.
	 */
	this.ma_instances_flattened = null;

	/*
	 * General-purpose information about each instance.  See InstanceInfo.
	 */
	this.ma_instance_info = null;

	/*
	 * Mapping of SAPI service names to the list of local (same-datacenter)
	 * instances for this service.
	 */
	this.ma_instances_local_bysvcname = null;

	/*
	 * CNAPI server objects, indexed by server_uuid.
	 */
	this.ma_cns = null;

	/*
	 * VMAPI vm objects for active VMs, indexed by instance uuid.
	 */
	this.ma_vms = null;

	/*
	 * VMAPI vm objects for destroyed VMs, indexed by instance uuid.  This
	 * is loaded only when loading alarm probes.
	 */
	this.ma_vms_destroyed = null;

	/*
	 * Set of server uuids hosting destroyed VMs.  This is loaded only when
	 * loading alarm probes.
	 */
	this.ma_cns_abandoned = null;

	/*
	 * Translation table from VM or CN uuid to the name of the service to
	 * which this instance belongs.  CNs, this is just "global zone".
	 * This only contains translations for the current datacenter.
	 */
	this.ma_instance_svcname = {};

	/*
	 * IMGAPI image objects, indexed by image uuid.
	 */
	this.ma_images = null;

	/*
	 * List of alarm sources that we initially collected data from.
	 */
	this.ma_alarm_sources = null;

	/*
	 * Amon alarm set
	 */
	this.ma_alarms = null;

	/*
	 * Deployed Amon configuration
	 */
	this.ma_amon_deployed = null;

	/*
	 * Warning-level issues encountered while loading alarms information.
	 */
	this.ma_alarm_warnings = [];

	/*
	 * Alarm metadata
	 */
	this.ma_alarm_metadata = null;

	/*
	 * Mapping of alarm levels to the Amon contacts to use for probe groups
	 * at that level.
	 */
	this.ma_alarm_levels = {};

	/*
	 * Information about global zones, indexed by server_uuid.  This is
	 * where we keep useful properties derived non-trivially from the CNAPI
	 * data.
	 */
	this.ma_gzinfo = null;

	/* Update information */
	this.ma_plan = null;
	this.ma_instances_wanted = null;
	this.ma_deployer = null;

	/*
	 * The summarized configuration is fundamentally a count of the number
	 * of services deployed for each "configuration" of that service.  For
	 * most services the "configuration" key is just the image: it's only
	 * interesting to know how many are deployed at each version.  For
	 * postgres and moray, the shard is also relevant, since instances in
	 * different shards are not interchangeable.
	 *
	 * The basic structure for keeping track of this for each service is an
	 * object that maps the configuration key (an array of keys, really) to
	 * the count of instances deployed for that key.  For example, for most
	 * services, this looks like:
	 *
	 *     {
	 *         "IMAGE_1_UUID": 6,
	 *         "IMAGE_2_UUID": 2
	 *     }
	 *
	 * while for "moray" and "postgres", this might look like:
	 *
	 *     {
	 *         "1": {
	 *             // Shard 1 configuration
	 *             "IMAGE_1_UUID": 2,
	 *             "IMAGE_2_UUID": 2
	 *         },
	 *         "2": {
	 *             // Shard 2 configuration
	 *             "IMAGE_1_UUID": 4
	 *         }
	 *     }
	 *
	 * It's useful to keep track of this in two different ways: in total,
	 * across the datacenter, as well as by-compute-node.
	 * this.ma_config_bycfg maps SAPI service uuids to one of these
	 * structures directly.  this.ma_config_bycn maps SAPI service uuids to
	 * a mapping of compute node uuids to these structures.
	 */
	this.ma_config_bycn = null;
	this.ma_config_bycfg = null;
}

maAdm.prototype.startOp = function ()
{
	var args = Array.prototype.slice.call(arguments);
	var msg = sprintf.apply(null, args);
	var id = this.ma_id++;

	this.ma_reqs[id] = {
	    'r_id': id,
	    'r_label': msg,
	    'r_start': Date.now(),
	    'r_done': null
	};

	return (id);
};

maAdm.prototype.doneOp = function (id)
{
	var op;

	assert.ok(this.ma_reqs.hasOwnProperty(id));

	op = this.ma_reqs[id];
	op.r_done = Date.now();

	delete (this.ma_reqs[id]);
	this.ma_recent.push(op);

	if (this.ma_recent.length > this.ma_recent_limit)
		this.ma_recent.shift();
};

/*
 * Initialize the SDC clients.
 */
maAdm.prototype.loadSdcConfig = function (callback)
{
	var self = this;
	var sdc, id;

	assert.ok(this.ma_sdc === null);
	id = this.startOp('load sdc config');
	sdc = { 'log': this.ma_log };
	this.ma_log.debug('initializing SDC clients');
	common.initSdcClients.call(sdc, function (err) {
		assert.ok(self.ma_sdc === null);
		self.doneOp(id);

		if (err) {
			callback(new VError(err, 'initializing SDC clients'));
			return;
		}

		self.ma_log.debug('initialized SDC clients');
		self.ma_sdc = sdc;
		callback();
	});
};

/*
 * Fetch the current state of deployed services.  This is the most expensive
 * operation since it requires a few trips to SAPI, VMAPI, and CNAPI.
 */
maAdm.prototype.fetchDeployed = function (callback)
{
	assert.ok(this.ma_sdc !== null, 'must load sdc config first');

	var self = this;
	var cns = this.ma_cns = {};
	this.ma_images = {};

	vasync.pipeline({
	    'funcs': [
		function fetchApp(_, stepcb) {
			var params = {
			    'name': self.ma_appname,
			    'include_master': true
			};

			self.ma_log.info('fetching "%s" application',
			    self.ma_appname);
			self.ma_sdc.SAPI.listApplications(params,
			    function (err, apps) {
				if (!err && apps.length < 1) {
					err = new VError(
					    'application not found: "%s"',
					    self.ma_appname);
				}

				if (err) {
					stepcb(new VError(
					    err, 'finding application "%s"',
					    self.ma_appname));
					return;
				}

				self.ma_app = apps[0];
				stepcb();
			    });
		},

		function fetchAppObjs(_, stepcb) {
			var params = { 'include_master': true };
			self.ma_log.info('fetching "%s" application objects',
			    self.ma_appname);
			self.ma_sdc.SAPI.getApplicationObjects(
			    self.ma_app.uuid, params, function (err, ret) {
				if (err) {
					stepcb(new VError(err,
					    'fetching application objects'));
					return;
				}

				self.ma_services = ret['services'];
				self.ma_instances = ret['instances'];
				stepcb();
			    });
		},

		/*
		 * TODO want a way to fetch all application instances from
		 * VMAPI.  We currently use owner_uuid as a proxy for that, but
		 * that's not necessarily correct.
		 */
		function fetchVmInfo(_, stepcb) {
			var params;

			assertplus.string(self.ma_app.owner_uuid);
			params = {
			    'state': 'active',
			    'owner_uuid': self.ma_app.owner_uuid
			};
			self.ma_log.info(params, 'listing VMs');
			self.ma_sdc.VMAPI.listVms(params, function (err, uvms) {
				if (err) {
					stepcb(new VError(err,
					    'listing VMs for user "%s"',
					    self.ma_app.owner_uuid));
					return;
				}

				self.ma_vms = {};
				uvms.forEach(function (vm) {
					if (!vm.tags.hasOwnProperty(
					    'manta_role'))
						return;
					self.ma_vms[vm['uuid']] = vm;
				});
				stepcb();
			});
		},

		function fetchCnInfo(_, stepcb) {
			var svcid, i, instance;

			for (svcid in self.ma_instances) {
				for (i = 0;
				    i < self.ma_instances[svcid].length; i++) {
					instance = self.ma_instances[svcid][i];
					if (self.ma_vms.hasOwnProperty(
					    instance['uuid'])) {
						cns[self.ma_vms[
						    instance['uuid']]
						    ['server_uuid']] = null;
					}
				}
			}

			self.fetchCnInfo(Object.keys(cns), stepcb);
		},

		function fetchImagesInfo(_, stepcb) {
			self.fetchImagesInfo(Object.keys(self.ma_services),
			    stepcb);
		},

		function loadFini(_, stepcb) {
			self.ma_log.info('loaded current deployed state');
			self.loadCns();
			self.loadInstances();
			stepcb();
		}
	    ]
	}, callback);
};

/*
 * [internal] Fetch details about the given list of compute nodes (specified by
 * server_uuids).
 */
maAdm.prototype.fetchCnInfo = function (cnids, callback)
{
	var self = this;
	var errors, queue;

	errors = [];
	self.ma_log.info('fetching info for CNs');
	queue = vasync.queue(function (cnid, subcallback) {
		self.ma_log.trace({ 'cnid': cnid }, 'fetching info for CN');
		self.ma_sdc.CNAPI.getServer(cnid, function (err, cn) {
			if (!err)
				self.ma_cns[cn.uuid] = cn;
			else if (err.name != 'ResourceNotFoundError')
				errors.push(
				    new VError(err, 'fetching CN "%s"', cnid));

			subcallback();
		});
	}, maMaxConcurrency);
	cnids.forEach(function (cnid) { queue.push(cnid); });
	queue.close();
	queue.on('end', function () {
		if (errors.length > 0)
			callback(errors[0]);
		else
			callback();
	});
};

/*
 * [internal] Fetch image information for the given list of services (specified
 * by service_uuids).
 */
maAdm.prototype.fetchImagesInfo = function (svcids, callback) {
	var self = this;
	var errors, queue, svc, params;
	errors = [];
	self.ma_log.info('fetching info for images');
	queue = vasync.queue(function (svcid, subcallback) {
		svc = self.ma_services[svcid];
		params = {
			'name': '~' + svc.name,
			'state': 'active',
			'tags.smartdc_service': 'true',
			'owner_uuid': self.ma_app.owner_uuid
		};
		self.ma_log.trace({ 'service': svc.name },
		    'fetching images for service');
		self.ma_sdc.IMGAPI.listImages(params, function (err, uimgs) {
			if (err) {
				errors.push(new VError(err,
				    'fetching images for service "%s"',
				    svc.name));
			} else if (uimgs.length) {
				uimgs.forEach(function (img) {
					self.ma_images[img.uuid] = img;
				});
			}
			subcallback();
		});
	}, maMaxConcurrency);
	queue.push(svcids);
	queue.close();
	queue.on('end', function () {
		if (errors.length > 0)
			callback(errors[0]);
		else
			callback();
	});
};

/*
 * [for testing only] Load a fake set of results from the SAPI, VMAPI, and CNAPI
 * services queried by fetchDeployed().
 */
maAdm.prototype.loadFakeDeployed = function (config)
{
	this.ma_app = config['app'];
	this.ma_services = config['services'];
	this.ma_instances = config['instances'];
	this.ma_vms = config['vms'];
	this.ma_cns = config['cns'];
	this.ma_images = config['images'];

	this.loadCns();
	this.loadInstances();
};

var schemaAlarmConfigLevel = {
    'type': 'array',
    'required': true,
    'items': {
	'type': 'string'
    }
};

var schemaAlarmConfig = {
    'type': 'object',
    'properties': {
	'levels': {
	    'type': 'object',
	    'required': true,
	    'additionalProperties': false,
	    'properties': {
		'alert': schemaAlarmConfigLevel,
		'info': schemaAlarmConfigLevel
	    }
	}
    }
};

/*
 * General-purpose function for loading alarm-related data.  There are several
 * different sources, some of which are expensive to gather, and callers must
 * specify the data sources they want to load from.  Currently, this function
 * should only be called once in the lifetime of this object.
 *
 * All of the following top-level named arguments are required:
 *
 *    concurrency               maximum number of concurrent requests to make
 *    (number)
 *
 *    configFile                sdc-manta configuration file, which is used for
 *    (string)                  the set of contacts used for alarms
 *
 *    sources                   describes which sources to load data from.  This
 *    (object)                  object may be empty.
 *
 *        configBasic           load probe group information, necessary for
 *        (boolean)             summarizing basic configuration
 *
 *        configFull            load probe information, necessary for actually
 *        (boolean)             verifying or updating configuration.  This
 *                              implicitly pulls in "configBasic" as well.
 *
 *        alarms                load alarm information.  Exactly one of "state"
 *        (object)              or "alarmIds" must be specified.
 *
 *            state             fetch all alarms in state "state"
 *            (string)
 *
 *            alarmIds          fetch the specified alarm ids
 *            (array of string)
 *
 * In all cases, even if "sources" is empty, local metadata related to alarms is
 * loaded.
 *
 * Callers should invoke alarmWarnings() after calling this to see if there were
 * any non-fatal issues associated with loading alarms.  Operators should
 * generally be notified about these issues (as warning-level messages).
 */
maAdm.prototype.alarmsInit = function (args, callback)
{
	var self = this;
	var account, configfile, concurrency, funcs, components;
	var alarmstate = null, alarmIds = null;

	assertplus.object(args, 'args');
	assertplus.object(args, 'args.sources');
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.string(args.configFile, 'args.configFile');
	assertplus.func(callback, 'callback');
	assertplus.strictEqual(this.ma_alarms, null);
	assertplus.strictEqual(this.ma_amon_deployed, null);
	assertplus.strictEqual(this.ma_alarm_metadata, null);

	assertplus.optionalBool(args.sources.configBasic);
	assertplus.optionalBool(args.sources.configFull);
	assertplus.optionalObject(args.sources.alarms);
	if (args.sources.alarms) {
		assertplus.optionalString(args.sources.alarms.state);
		if (typeof (args.sources.alarms.state) == 'string') {
			if (args.sources.alarms.state != 'open' &&
			    args.sources.alarms.state != 'all' &&
			    args.sources.alarms.state != 'recent' &&
			    args.sources.alarms.state != 'closed') {
				throw (new VError('unsupported alarm state: ' +
				    '"%s"', args.sources.alarms.state));
			}

			assertplus.ok(
			    !args.sources.alarms.hasOwnProperty('alarmIds'),
			    'cannot specify "sources.alarms.state" and ' +
			    '"sources.alarms.alarmIds"');
			alarmstate = args.sources.alarms.state;
		} else {
			assertplus.arrayOfString(args.sources.alarms.alarmIds,
			    'must specify "sources.alarms.state" or ' +
			    '"sources.alarms.alarmIds');
			alarmIds = args.sources.alarms.alarmIds;
		}
	}

	this.ma_alarm_sources = jsprim.deepCopy(args.sources);
	configfile = args.configFile;
	concurrency = args.concurrency;
	funcs = [];

	/*
	 * We always want to load the configuration file.
	 */
	funcs.push(function loadConfigFile(_, stepcb) {
		fs.readFile(configfile, function onFileRead(err, contents) {
			var conf;

			if (err) {
				err = new VError(err, 'read "%s"', configfile);
				stepcb(err);
				return;
			}

			try {
				conf = JSON.parse(contents.toString('utf8'));
			} catch (ex) {
				err = new VError(ex, 'parse "%s"', configfile);
				stepcb(err);
				return;
			}

			err = jsprim.validateJsonObject(
			    schemaAlarmConfig, conf);
			if (err !== null) {
				stepcb(new VError(
				    err, 'config "%s"', configfile));
				return;
			}

			self.ma_alarm_levels['minor'] = conf.levels.info;
			self.ma_alarm_levels['major'] = conf.levels.alert;
			self.ma_alarm_levels['critical'] = conf.levels.alert;
			stepcb();
		});
	});

	/*
	 * We always want to load metadata.
	 */
	funcs.push(function loadMetadata(_, stepcb) {
		alarms.loadMetadata({
		    'directory': maAlarmMetadataDirectory
		}, function onAlarmMetadataLoaded(err, metadata) {
			if (!err) {
				self.ma_alarm_metadata = metadata;
			}

			stepcb(err);
		});
	});

	/*
	 * If the user asked for "configBasic" or "configFull", then we need the
	 * list of probe groups.
	 */
	if (args.sources.configBasic || args.sources.configFull) {
		assert.ok(this.ma_instances !== null,
		    'must load deployed first');
		account = this.ma_app.owner_uuid;
		funcs.push(function fetchProbeGroups(_, stepcb) {
			alarms.amonLoadProbeGroups({
			    'amon': self.ma_sdc.AMON,
			    'account': account
			}, function (err, amonconfig) {
				/*
				 * This function can emit both an error and a
				 * result.  In that case, the error represents
				 * non-fatal (warning-level) issues associated
				 * with the operation.
				 */
				if (amonconfig) {
					self.ma_amon_deployed = amonconfig;
				}

				if (err) {
					VError.errorForEach(err,
					    function (e) {
						self.ma_alarm_warnings.push(e);
					    });
				}

				stepcb();
			});
		});
	}

	/*
	 * If the user asked for "configFull", then we additionally need the
	 * list of probes.  In order to gather a complete list of those, we need
	 * to also list all of the VMs (and associated CNs) that have been
	 * destroyed.  This is deeply unfortunate, since it means that this
	 * operation will take time proportional to the total number of poseidon
	 * VMs ever deployed.  But in the absence of pagination from the Amon
	 * APIs, this is the only way we can identify the case of probes
	 * deployed for zones that no longer exist.
	 */
	if (args.sources.configFull) {
		var havecns = {};

		components = Object.keys(this.ma_vms).map(function (vmuuid) {
			return ({ 'type': 'vm', 'uuid': vmuuid });
		}).concat(Object.keys(this.ma_cns).map(function (cnuuid) {
			havecns[cnuuid] = true;
			return ({ 'type': 'cn', 'uuid': cnuuid });
		}));

		funcs.push(function fetchDestroyedVms(_, stepcb) {
			var params;

			assertplus.string(self.ma_app.owner_uuid);
			params = {
			    'state': 'destroyed',
			    'owner_uuid': self.ma_app.owner_uuid
			};
			self.ma_sdc.VMAPI.listVms(params, function (err, uvms) {
				if (err) {
					stepcb(new VError(err,
					    'listing destroyed VMs'));
					return;
				}

				uvms = uvms.filter(function (vm) {
					return (vm.tags.hasOwnProperty(
					    'manta_role'));
				});

				assertplus.strictEqual(self.ma_vms_destroyed,
				    null);
				self.ma_vms_destroyed = {};
				assertplus.strictEqual(self.ma_cns_abandoned,
				    null);
				self.ma_cns_abandoned = {};
				uvms.forEach(function (vm) {
					self.ma_vms_destroyed[vm.uuid] = vm;
					components.push({
					    'type': 'vm',
					    'uuid': vm.uuid
					});

					if (typeof (vm.server_uuid) ==
					    'string' && !havecns.hasOwnProperty(
					    vm.server_uuid)) {
						self.ma_cns_abandoned[
						    vm.server_uuid] = true;
						havecns[vm.server_uuid] = true;
						components.push({
						    'type': 'cn',
						    'uuid': vm.server_uuid
						});
					}
				});

				stepcb();
			});
		});

		funcs.push(function fetchProbes(_, stepcb) {
			assertplus.notStrictEqual(self.ma_amon_deployed, null);

			/*
			 * This function inserts the probe information into
			 * self.ma_amon_deployed, so we don't need to do
			 * anything when it completes.
			 */
			alarms.amonLoadComponentProbes({
			    'amonRaw': self.ma_sdc.AMON_RAW,
			    'amoncfg': self.ma_amon_deployed,
			    'concurrency': concurrency,
			    'components': components
			}, function (err, warnings) {
				if (warnings) {
					VError.errorForEach(warnings,
					    function (e) {
						self.ma_alarm_warnings.push(e);
					    });
				}

				stepcb(err);
			});
		});
	}

	/*
	 * Finally, if the user asked for alarms, then fetch them as requested.
	 */
	if (alarmstate !== null) {
		assert.ok(this.ma_instances !== null,
		    'must load deployed first');
		account = this.ma_app.owner_uuid;
		funcs.push(function fetchAlarms(_, stepcb) {
			alarms.amonLoadAlarmsForState({
			    'amon': self.ma_sdc.AMON,
			    'account': account,
			    'state': alarmstate
			}, function (err, alarmset) {
				if (!alarmset) {
					stepcb(err);
					return;
				}

				self.ma_alarms = alarmset;
				if (err) {
					VError.errorForEach(err, function (e) {
						self.ma_alarm_warnings.push(e);
					});
				}
				stepcb();
			});
		});
	} else if (alarmIds !== null) {
		assert.ok(this.ma_instances !== null,
		    'must load deployed first');
		account = this.ma_app.owner_uuid;
		funcs.push(function fetchAlarmIds(_, stepcb) {
			alarms.amonLoadAlarmsForIds({
			    'amon': self.ma_sdc.AMON,
			    'account': account,
			    'alarmIds': alarmIds,
			    'concurrency': concurrency
			}, function (err, alarmset) {
				/*
				 * This function can return warnings (in "err")
				 * as well as a list of alarms.
				 */
				assertplus.strictEqual(self.ma_alarms, null);
				self.ma_alarms = alarmset;
				if (err) {
					VError.errorForEach(err, function (e) {
						self.ma_alarm_warnings.push(e);
					});
				}
				stepcb();
			});
		});
	}

	vasync.pipeline({
	    'funcs': funcs
	}, function (err) {
		callback(err);
	});
};

maAdm.prototype.alarmWarnings = function ()
{
	return (this.ma_alarm_warnings);
};

/*
 * Show information about fetched alarms.  This is intended to summarize each
 * alarm in a multi-line form suitable for people (not programs).
 */
maAdm.prototype.alarmsShow = function (args)
{
	var self = this;
	var out;

	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configBasic" source first');
	assertplus.notStrictEqual(this.ma_alarms, null,
	    'must call alarmsInit() with "alarms" source first');

	out = args.stream;
	this.ma_alarms.eachAlarm(function (id, alarm) {
		var details;
		details = self.alarmDetails(alarm);
		fprintf(out, 'ALARM %-6d  %-8s  %s\n', alarm.a_id,
		    details.ka !== null ?
		    details.ka.ka_severity.toUpperCase() : 'UNKNOWN',
		    details.summary);
		fprintf(out, '    %d event%s (last: %s)\n',
		    alarm.a_nevents,
		    alarm.a_nevents == 1 ? ' ' : 's',
		    alarm.a_time_last.toISOString());
		fprintf(out, '    affects services: %s\n',
		    details.affects.join(', '));
		if (alarm.a_suppressed) {
			fprintf(out, '    NOTE: notifications are disabled ' +
			    'for this alarm\n');
		}
		alarm.a_faults.forEach(function (f) {
			var faultSummary = self.faultSummary(f);
			if (faultSummary.messageSummary !== null) {
				fprintf(out, '    message: %s\n',
				    JSON.stringify(
				    faultSummary.messageSummary));
			}
		});
		fprintf(out, '\n');
	});
};

/*
 * List open alarms.  This is a tabular summary of alarms, one alarm per line,
 * with selectable columns.  This may be used by people or by programs.
 */
maAdm.prototype.alarmsList = function (args)
{
	var self = this;
	var rows;
	var nnoprobegroup = 0;
	var nbadprobegroup = 0;

	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.optionalArrayOfString(args.columns, 'args.columns');
	assertplus.bool(args.omitHeader, 'args.omitHeader');

	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configBasic" source first');
	assertplus.notStrictEqual(this.ma_alarms, null,
	    'must call alarmsInit() with "alarms" source first');
	rows = [];
	this.ma_alarms.eachAlarm(function (id, alarm) {
		var details;

		details = self.alarmDetails(alarm);
		if (details.nogroup) {
			nnoprobegroup++;
		}
		if (details.badgroup) {
			nbadprobegroup++;
		}

		rows.push({
		    'ALARM': id,
		    'DATE_OPENED': fmtDateOnly(alarm.a_time_opened),
		    'TIME_OPENED': fmtListDateTime(alarm.a_time_opened),
		    'DATE_CLOSED': fmtDateOnly(alarm.a_time_closed),
		    'TIME_CLOSED': fmtListDateTime(alarm.a_time_closed),
		    'DATE_LAST': fmtDateOnly(alarm.a_time_last),
		    'TIME_LAST': fmtListDateTime(alarm.a_time_last),
		    'NFLTS': alarm.a_faults.length,
		    'NEVENTS': alarm.a_nevents,
		    'NFY': alarm.a_suppressed ? 'no' : 'yes',
		    'SUMMARY': details.summary
		});
	});

	this.doList({
	    'stream': args.stream,
	    'columnsSelected': args.columns,
	    'columnsDefault': [ 'alarm', 'dateLast', 'nflts', 'summary' ],
	    'columnMetadata': maAlarmColumns,
	    'rows': rows,
	    'omitHeader': args.omitHeader
	});

	if (nnoprobegroup) {
		console.error('note: %d alarm%s %s not associated with ' +
		    'probe groups', nnoprobegroup,
		    nnoprobegroup == 1 ? '' : 's',
		    nnoprobegroup == 1 ? 'was' : 'were');
	}

	if (nbadprobegroup) {
		console.error('note: %d alarm%s %s associated with ' +
		    'non-existent probe groups', nbadprobegroup,
		    nbadprobegroup == 1 ? '' : 's',
		    nbadprobegroup == 1 ? 'was' : 'were');
	}
};

/*
 * Prints a tabular summary of configured probe groups.
 */
maAdm.prototype.alarmsProbeGroupsList = function (args)
{
	var self = this;
	var nalarmsByGroup, rows;

	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.optionalArrayOfString(args.columns, 'args.columns');
	assertplus.bool(args.omitHeader, 'args.omitHeader');
	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configBasic" source first');
	assertplus.notStrictEqual(this.ma_alarms, null,
	    'must call alarmsInit() with "alarms" source first');

	/*
	 * First, count the alarms for each probe group.
	 */
	nalarmsByGroup = {};
	this.ma_alarms.eachAlarm(function (id, aa) {
		if (aa.a_groupid === null) {
			return;
		}

		assertplus.string(aa.a_groupid);
		if (!nalarmsByGroup.hasOwnProperty(aa.a_groupid)) {
			nalarmsByGroup[aa.a_groupid] = 0;
		}

		nalarmsByGroup[aa.a_groupid]++;
	});

	/*
	 * Construct an output row for each probe group.
	 */
	rows = [];
	this.ma_amon_deployed.eachProbeGroup(function (pg) {
		var row, nprobes, nalarms;

		nprobes = 0;
		self.ma_amon_deployed.eachProbeGroupProbe(pg.pg_name,
		    function () { nprobes++; });

		nalarms = nalarmsByGroup.hasOwnProperty(pg.pg_uuid) ?
		    nalarmsByGroup[pg.pg_uuid] : 0;

		row = {
		    'NAME': pg.pg_name,
		    'UUID': pg.pg_uuid,
		    'CONTACTS': pg.pg_contacts.join(','),
		    'NPROBES': nprobes,
		    'NALARMS': nalarms,
		    'ENAB': pg.pg_enabled ? 'yes' : 'no'
		};

		rows.push(row);
	});

	this.doList({
	    'stream': args.stream,
	    'columnsSelected': args.columns,
	    'columnsDefault': [ 'uuid', 'name' ],
	    'columnMetadata': maProbeGroupColumns,
	    'rows': rows,
	    'omitHeader': args.omitHeader
	});
};

/*
 * Close a list of alarms, each identified by id.  Invalid alarm ids are
 * operational errors here (resulting in warnings).
 */
maAdm.prototype.alarmsClose = function (args, callback)
{
	assertplus.object(args, 'args');
	assertplus.arrayOfString(args.alarmIds, 'args.alarmIds');
	assertplus.number(args.concurrency, 'args.concurrency');

	alarms.amonCloseAlarms({
	    'amon': this.ma_sdc.AMON,
	    'account': this.ma_app.owner_uuid,
	    'alarmIds': args.alarmIds,
	    'concurrency': args.concurrency
	}, callback);
};

/*
 * Enable or disable notifications for a list of alarms, each identified by id.
 * Invalid alarm ids are operational errors here (resulting in warnings).
 */
maAdm.prototype.alarmsUpdateNotification = function (args, callback)
{
	assertplus.object(args, 'args');
	assertplus.arrayOfString(args.alarmIds, 'args.alarmIds');
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.bool(args.suppressed, 'args.suppressed');

	alarms.amonUpdateAlarmsNotification({
	    'amonRaw': this.ma_sdc.AMON_RAW,
	    'account': this.ma_app.owner_uuid,
	    'alarmIds': args.alarmIds,
	    'concurrency': args.concurrency,
	    'suppressed': args.suppressed
	}, callback);
};

/*
 * Fetch a structure summarizing information about a specific alarm.  The
 * structure has properties:
 *
 *     badgroup         indicates that the associated probe group is missing
 *     (boolean)
 *
 *     nogroup          indicates that there is no probe group for this alarm
 *     (boolean)
 *
 *     affects          array of service names whose instances have faults
 *                      associated with this alarm.
 *
 *     summary          one-line string summary of the alarm.  We prefer
 *                      information provided in the associated knowledge
 *                      article, if there is one.  Otherwise, we use the probe
 *                      group name if we have one.  Otherwise, we generate a
 *                      summary from a fault associated with the alarm.
 */
maAdm.prototype.alarmDetails = function (alarm)
{
	var self = this;
	var rv, pgid, pgname, eventName;
	var deployed, metadata, svcnames;

	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configBasic" source first');
	deployed = this.ma_amon_deployed;
	metadata = this.ma_alarm_metadata;
	pgid = alarm.a_groupid;
	rv = {
	    'badgroup': false,
	    'nogroup': false,
	    'affects': null,
	    'summary': null,
	    'ka': null
	};

	/*
	 * Figure out the most specific summary message we can provide given the
	 * local metadata and probe group information.
	 */
	if (pgid !== null) {
		pgname = deployed.probeGroupNameForUuid(pgid);
		if (pgname !== null) {
			eventName = metadata.probeGroupEventName(
			    pgname);
			if (rv.eventName !== null) {
				rv.ka = metadata.eventKa(eventName);
				if (rv.ka !== null) {
					rv.summary = rv.ka.ka_title;
				} else {
					rv.summary = eventName;
				}
			} else {
				rv.summary = pgname;
			}
		} else {
			rv.badgroup = true;
		}
	} else {
		rv.nogroup = true;
	}

	if (rv.summary === null) {
		/*
		 * Open alarms should have at least one fault, and closed alarms
		 * should have none.  This is verified on the way in from Amon.
		 */
		if (alarm.a_faults.length > 0) {
			rv.summary = alarm.a_faults[0].aflt_summary;
		} else {
			assertplus.ok(alarm.a_closed);
			rv.summary = '(closed alarm has no faults)';
		}
	}

	/*
	 * Determine the affected services by looking at all of the faults.
	 */
	svcnames = {};
	alarm.a_faults.forEach(function (f) {
		svcnames[
		    self.ma_instance_svcname.hasOwnProperty(f.aflt_agent) ?
		    self.ma_instance_svcname[f.aflt_agent] :
		    'unknown'] = true;
	});

	if (alarm.a_faults.length === 0) {
		svcnames['none (alarm is closed)'] = true;
	}

	rv.affects = Object.keys(svcnames);

	return (rv);
};

/*
 * Returns a structure summarizing a specific fault.  The returned object has
 * properties:
 *
 *    kind	      one of "cmd", "log-scan", "bunyan-log-scan", or "unknown"
 *                    describing the kind of probe that generated this fault
 *                    (see below)
 *
 *    messageSummary  for the log-scan types, this is the shortest reasonable
 *                    summary of the log message that generated the fault
 *
 *    messageWhole    for one of the log-scan types, this is the entire contents
 *                    of the message that generated the fault
 *
 * Manta uses four different types of probes:
 *
 *   - "bunyan-log-scan" (scans a file for messages at a specified level)
 *   - "cmd" (runs a command periodically)
 *   - "disk-usage (checks disk space used)
 *   - "log-scan" (scans a file for a pattern)
 *
 * We can encounter faults for any of these types of probe, and we want to print
 * each one differently.  We could reliably tell which type we're looking at by
 * looking up the probe, but it's expensive to fetch information about all
 * probes, so we don't typically do it just to show alarm details.  Instead, we
 * use heuristics about the information provided in the fault to guess what kind
 * we're looking at.  This function abstracts that behavior.
 */
maAdm.prototype.faultSummary = function (fault)
{
	var fltdetail, rv;

	rv = {
	    'kind': 'unknown',
	    'messageSummary': null,
	    'messageWhole': null
	};

	fltdetail = fault.aflt_data.details;
	if (!fltdetail) {
		return (rv);
	}

	if (typeof (fltdetail.cmd) == 'string' &&
	    fltdetail.hasOwnProperty('stdout') &&
	    fltdetail.hasOwnProperty('stderr')) {
		/* This is a "cmd" probe. */
		rv.kind = 'cmd';
	} else if (Array.isArray(fltdetail.matches) &&
	    fltdetail.matches.length > 0) {
		if (typeof (fltdetail.matches[0].context) == 'string') {
			/*
			 * This is a "log-scan" probe, and the "context" field
			 * indicates the line that matched.
			 */
			rv.kind = 'log-scan';
			rv.messageWhole = fltdetail.matches[0].context;
			rv.messageSummary = rv.messageWhole;
		} else if (typeof (fltdetail.matches[0].match) == 'object') {
			/*
			 * This is a "bunyan-log-scan" probe, and the "match"
			 * field indicates the entire bunyan record that
			 * matched.  Note that "log-scan" probes also have a
			 * "match" field, and it means something else, so it's
			 * important that we checked for this after checking for
			 * "context" above.
			 */
			rv.kind = 'bunyan-log-scan';
			rv.messageSummary = fltdetail.matches[0].match.msg;
			rv.messageWhole = JSON.stringify(
			    fltdetail.matches[0].match, null, '    ');
		}
	}

	return (rv);
};

/*
 * Prints detailed information about a specific alarm that has already been
 * loaded, identified by id.  If the alarm has not been loaded, an error is
 * returned.
 */
maAdm.prototype.alarmPrint = function alarmPrint(args)
{
	var alarm, out, i, nmax;
	var details, fault, fltdetail, fltsum, formatted;
	var now = Date.now();

	assertplus.object(args, 'args');
	assertplus.string(args.id, 'args.id');
	assertplus.object(args.stream, 'args.stream');
	assertplus.optionalNumber(args.nmaxfaults, 'args.nmaxfaults');
	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configBasic" source first');

	out = args.stream;
	alarm = this.ma_alarms.alarmForId(args.id);
	if (alarm === null) {
		return (new VError('no such alarm: "%s"', args.id));
	}

	details = this.alarmDetails(alarm);
	if (details.ka !== null) {
		this.doKaPrint({
		    'header': 'ALARM ' + args.id,
		    'stream': out,
		    'ka': details.ka
		});
		fprintf(out, '\n');
	} else {
		fprintf(out, 'ALARM %s\n', alarm.a_id);
	}

	fprintf(out, 'summary:         %s\n', details.summary);
	fprintf(out, 'state:           %s\n', alarm.a_closed ?
	    'closed': 'open');
	fprintf(out, 'opened:          %s (%s ago)\n',
	    alarm.a_time_opened.toISOString(),
	    fmtDuration(now - alarm.a_time_opened.getTime()));
	fprintf(out, 'last event:      %s (%s ago)\n',
	    alarm.a_time_last.toISOString(),
	    fmtDuration(now - alarm.a_time_last.getTime()));
	if (alarm.a_time_closed === null) {
		fprintf(out, 'closed:          never\n');
	} else {
		fprintf(out, 'closed:          %s (%s ago)\n',
		    alarm.a_time_closed.toISOString(),
		    fmtDuration(now - alarm.a_time_closed.getTime()));
	}
	fprintf(out, 'notifications:   %s\n', alarm.a_suppressed ?
	    'disabled' : 'enabled');
	fprintf(out, 'total faults:    %s\n', alarm.a_faults.length);
	fprintf(out, 'total events:    %s\n', alarm.a_nevents);
	fprintf(out, 'affects zones:   %s\n', details.affects.join(', '));

	if (alarm.a_faults.length === 0) {
		return (null);
	}

	if (typeof (args.nmaxfaults) == 'number') {
		nmax = Math.min(alarm.a_faults.length, args.nmaxfaults);
	} else {
		nmax = alarm.a_faults.length;
	}

	for (i = 0; i < nmax; i++) {
		fprintf(out, '\n    FAULT %d of %d FOR ALARM %d\n',
		    i + 1, alarm.a_faults.length, alarm.a_id);
		fault = alarm.a_faults[i];
		fprintf(out, '    reason:          %s\n', fault.aflt_summary);
		fprintf(out, '    time:            %s (%s ago)\n',
		    fault.aflt_time.toISOString(),
		    fmtDuration(now - fault.aflt_time.getTime()));
		fprintf(out, '    machine:         %s\n', fault.aflt_machine);
		fprintf(out, '    agent:           %s\n', fault.aflt_agent);
		fprintf(out, '    agent alias:     %s\n',
		    fault.aflt_agent_alias);

		/*
		 * It's not expected that we would ever see a fault with "clear"
		 * set to true.  If Amon received that, it would have closed the
		 * fault and removed it from the list of faults.
		 */
		if (fault.aflt_clear) {
			fprintf(out, '    warn: fault event has "clear" set\n');
		}

		fltsum = this.faultSummary(fault);
		if (fltsum.kind != 'cmd' &&
		    fltsum.kind != 'log-scan' &&
		    fltsum.kind != 'bunyan-log-scan') {
			continue;
		}

		fltdetail = fault.aflt_data.details;
		if (fltsum.kind == 'cmd') {
			/* This is a "cmd" probe. */
			fprintf(out, '    cmd exit status: %s\n',
			    (fltdetail.exitStatus ||
			    fltdetail.exitStatus === 0) ?
			    fltdetail.exitStatus : 'none');
			fprintf(out, '    cmd signal:      %s\n',
			    fltdetail.signal ? fltdetail.signal : 'none');
			fprintf(out, '    probe cmd:       %s\n',
			    JSON.stringify(fltdetail.cmd));

			fprintf(out, '%s', formatCmdOutput(
			    '    ', 'stdout', fltdetail.stdout));
			fprintf(out, '%s', formatCmdOutput(
			    '    ', 'stderr', fltdetail.stderr));
		} else if (fltsum.kind == 'log-scan') {
			/* This is a "log-scan" probe. */
			fprintf(out, '    first matching message:\n');
			fprintf(out, '    ------------\n');
			fprintf(out, '    %s\n',
			    JSON.stringify(fltsum.messageWhole));
			fprintf(out, '    ------------\n');
		} else if (fltsum.kind == 'bunyan-log-scan') {
			/* This is a "bunyan-log-scan" probe. */
			formatted = prependLines(fltsum.messageWhole, '    ');
			fprintf(out, '    first matching message:\n');
			fprintf(out, '    ------------\n');
			fprintf(out, '%s\n', formatted);
			fprintf(out, '    ------------\n');
		}

		/*
		 * The only other type of probe currently in use is the
		 * disk-usage probe, and it has no additional information than
		 * what's in the message that we already printed out.
		 */
	}

	return (null);
};

/*
 * Returns an array of all known event names (based on alarm metadata).  Each of
 * these identifies a specific failure mode for which we create probes.
 */
maAdm.prototype.alarmEventNames = function alarmEventNames()
{
	assertplus.notStrictEqual(this.ma_alarm_metadata, null,
	    'must call alarmsInit() first');
	var rv = [];
	this.ma_alarm_metadata.eachEvent(function (eventName) {
		rv.push(eventName);
	});
	return (rv);
};

/*
 * Prints the contents of a specific knowledge article, identified by its event
 * name.
 */
maAdm.prototype.alarmKaPrint = function alarmKaPrint(args)
{
	var ka, eventName;

	assertplus.notStrictEqual(this.ma_alarm_metadata, null,
	    'must call alarmsInit() first');

	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.string(args.eventName, 'args.eventName');

	eventName = args.eventName;
	ka = this.ma_alarm_metadata.eventKa(eventName);
	if (ka === null) {
		return (new VError('no such event: "%s"', eventName));
	}

	this.doKaPrint({
	    'stream': args.stream,
	    'ka': ka
	});
};

maAdm.prototype.doKaPrint = function doKaPrint(args)
{
	var out, ka, header;
	var wrapper = wordwrap(4, 80);

	assertplus.object(args, 'args');
	assertplus.object(args.ka, 'args.ka');
	assertplus.object(args.stream, 'args.stream');
	assertplus.optionalString(args.header, 'args.header');

	ka = args.ka;
	out = args.stream;
	header = args.header ? args.header : 'TITLE';
	fprintf(out, '%s: %s\n', header, ka.ka_title);
	fprintf(out, 'SEVERITY: %s\n', ka.ka_severity);
	fprintf(out, 'DESC:\n%s\n', wrapper(ka.ka_description));
	fprintf(out, 'IMPACT:\n%s\n', wrapper(ka.ka_impact));
	fprintf(out, 'AUTOMATED RESPONSE:\n%s\n', wrapper(ka.ka_response));
	fprintf(out, 'SUGGESTED ACTION:\n%s\n', wrapper(ka.ka_action));
};

/*
 * General-purpose function for printing tabular output.
 */
maAdm.prototype.doList = function doList(args)
{
	var colnames, columns, taboptions, tabstream;

	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.optionalArrayOfString(
	    args.columnsSelected, 'args.columnsSelected');
	assertplus.arrayOfString(args.columnsDefault, 'args.columnsDefault');
	assertplus.object(args.columnMetadata, 'args.columnMetadata');
	assertplus.arrayOfObject(args.rows, 'args.rows');
	assertplus.optionalBool(args.omitHeader, 'args.omitHeader');

	colnames = args.columnsSelected || args.columnsDefault;
	columns = colnames.map(function (colname) {
		colname = colname.toLowerCase();
		assertplus.ok(args.columnMetadata.hasOwnProperty(colname),
		    'no property "' + colname + '"');
		return (args.columnMetadata[colname]);
	});
	taboptions = {
	    'stream': args.stream,
	    'omitHeader': args.omitHeader,
	    'columns': columns
	};
	tabstream = new tab.TableOutputStream(taboptions);
	args.rows.forEach(function (row) {
		tabstream.writeRow(row);
	});
};

/*
 * Print a summary about the currently configured probe groups and probes.
 */
maAdm.prototype.alarmConfigShow = function (args)
{
	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configBasic" source first');
	alarms.amonConfigSummarize({
	    'config': this.ma_amon_deployed,
	    'metadata': this.ma_alarm_metadata,
	    'instanceSvcname': this.ma_instance_svcname,
	    'stream': args.stream
	});
};

/*
 * Create a plan for updating the Amon configuration.
 */
maAdm.prototype.amonUpdatePlanCreate = function amonUpdatePlanCreate(options)
{
	assertplus.notStrictEqual(this.ma_amon_deployed, null,
	    'must call alarmsInit() with "configFull" source first');
	assertplus.object(options, 'args');
	assertplus.bool(options.unconfigure, 'options.unconfigure');

	return (alarms.amonUpdatePlanCreate({
	    'account': this.ma_app.owner_uuid,
	    'contactsBySeverity': this.ma_alarm_levels,
	    'metadata': this.ma_alarm_metadata,
	    'instances': this.ma_instance_info,
	    'instancesBySvc': this.ma_instances_local_bysvcname,
	    'deployed': this.ma_amon_deployed,
	    'unconfigure': options.unconfigure
	}));
};

/*
 * Summarize a plan for updating the Amon configuration.
 */
maAdm.prototype.amonUpdatePlanDump = function amonUpdatePlanDump(args)
{
	assertplus.object(args, 'args');
	assertplus.object(args.plan, 'args.plan');
	assertplus.object(args.stream, 'args.stream');
	assertplus.bool(args.verbose, 'args.verbose');

	alarms.amonUpdatePlanSummarize({
	    'stream': args.stream,
	    'plan': args.plan,
	    'instances': this.ma_instance_info,
	    'cns': this.ma_cns,
	    'metadata': this.ma_alarm_metadata,
	    'verbose': args.verbose,
	    'vmsDestroyed': this.ma_vms_destroyed,
	    'cnsAbandoned': this.ma_cns_abandoned
	});
};

/*
 * Execute a plan for updating the Amon configuration.
 */
maAdm.prototype.amonUpdatePlanApply =
    function amonUpdatePlanApply(args, callback)
{
	assertplus.object(args, 'args');
	assertplus.object(args.plan, 'args.plan');
	assertplus.object(args.stream, 'args.stream');
	assertplus.number(args.concurrency, 'args.concurrency');

	alarms.amonUpdatePlanApply({
	    'account': this.ma_app.owner_uuid,
	    'amon': this.ma_sdc.AMON,
	    'concurrency': args.concurrency,
	    'stream': args.stream,
	    'plan': args.plan
	}, callback);
};

/*
 * The dumpConfig.* functions dump sample configurations based on common
 * deployments in development and test.
 */

maAdm.prototype.dumpConfigCoal = function (options, callback)
{
	return (this.dumpConfigCommon(options, {
	    'shards': [ '1' ],
	    'nameservice': 1,
	    'postgres': 2,
	    'moray': 1,
	    'electric-moray': 1,
	    'storage': 2,
	    'authcache': 1,
	    'webapi': 1,
	    'loadbalancer': 1,
	    'jobsupervisor': 1,
	    'jobpuller': 1,
	    'medusa': 1,
	    'ops': 1,
	    'marlin': 2
	}, callback));
};

maAdm.prototype.dumpConfigLab = function (options, callback)
{
	return (this.dumpConfigCommon(options, {
	    'shards': [ '1', '2' ],
	    'nameservice': 3,
	    'postgres': 3,
	    'moray': 3,
	    'electric-moray': 1,
	    'storage': 3,
	    'authcache': 2,
	    'webapi': 2,
	    'loadbalancer': 2,
	    'jobsupervisor': 2,
	    'jobpuller': 1,
	    'medusa': 2,
	    'ops': 1,
	    'madtom': 1,
	    'marlin-dashboard': 1,
	    'marlin': 10
	}, callback));
};

maAdm.prototype.dumpConfigCommon = function (options, conf, callback)
{
	var self = this;
	var sout = options.outstream;

	/*
	 * COAL and lab configurations are hardcoded to use the headnode for
	 * provisioning.  Use CNAPI to find the appropriate uuid.
	 */
	self.ma_sdc.CNAPI.listServers({ 'headnode': true },
	    function (err, servers) {
		var nwarnings, server_uuid;

		if (!err &&
		    (!servers || !Array.isArray(servers) ||
		    servers.length != 1 ||
		    servers[0].headnode !== true)) {
			err = new VError('expected array of exactly 1 ' +
			    'headnode server');
		}

		if (err) {
			callback(err);
			return;
		}

		server_uuid = servers[0].uuid;
		nwarnings = self.dumpConfigCommonFini(conf, sout, server_uuid);
		callback(null, nwarnings);
	    });
};

maAdm.prototype.latestImagesByService = function ()
{
	var imagesbysvcname, svcid, svc;

	assert.ok(this.ma_instances_flattened !== null, 'must load first');
	imagesbysvcname = {};
	for (svcid in this.ma_services) {
		svc = this.ma_services[svcid];
		imagesbysvcname[svc['name']] = svc['params']['image_uuid'];
	}

	return (imagesbysvcname);
};

maAdm.prototype.dumpConfigCommonFini = function (conf, sout, serverUuid)
{
	var config, imagesbysvcname;
	var nwarnings = 0;
	var toprint;

	imagesbysvcname = this.latestImagesByService();

	config = {};
	svcs.mSvcNames.forEach(function (svcname) {
		var image;

		if (!imagesbysvcname.hasOwnProperty(svcname)) {
			nwarnings++;
			console.error(
			    'warning: no image found for service "%s" ' +
			    '(skipped)', svcname);
			return;
		}

		image = imagesbysvcname[svcname];
		config[svcname] = {};
		if (svcs.serviceIsSharded(svcname)) {
			conf['shards'].forEach(function (shard) {
				config[svcname][shard] = {};
				config[svcname][shard][image] = conf[svcname];
			});
		} else {
			config[svcname][image] = conf[svcname];
		}
	});

	toprint = {};
	toprint[serverUuid] = config;
	sout.write(JSON.stringify(toprint, null, '    ') + '\n');
	return (nwarnings);
};

/*
 * Given a server configuration file, suggest a layout of Manta services across
 * that file.  See manta-adm(1).  The callback is invoked with an explicit
 * error for problems unrelated to the layout itself, and a count of errors
 * related to the layout.  These errors have already been written to
 * "errstream".
 *
 * Either "outstream" (a stream) or "outDirectory" (a string) should be
 * specified to indicate where the generated output should go.
 */
maAdm.prototype.genconfigFromFile = function (args, callback)
{
	var images, filename, outdir, outstream, errstream;
	var svclayout;

	assertplus.object(args, 'args');
	assertplus.string(args.filename, 'args.filename');
	assertplus.optionalString(args.outDirectory, 'args.outDirectory');
	assertplus.optionalObject(args.outstream, 'args.outstream');
	assertplus.object(args.errstream, 'args.errstream');

	assertplus.ok(typeof (args.outDirectory) == 'string' ||
	    (typeof (args.outstream) == 'object' && args.outstream !== null),
	    'at least one of "outDirectory" or "outstream" must be specified');

	images = this.latestImagesByService();
	filename = args.filename;
	outdir = args.outDirectory;
	outstream = args.outstream;
	errstream = args.errstream;

	return (vasync.waterfall([
	    function loadDcConfig(subcallback) {
		var loader = new layout.DcConfigLoader();
		loader.loadFromFile({
		    'filename': filename
		}, subcallback);
	    },

	    function generate(dcconfig, subcallback) {
		var azs, generated;

		svclayout = layout.generateLayout({
		    'dcconfig': dcconfig,
		    'images': images
		});

		/*
		 * We don't need to propagate errors directly because
		 * printIssues() will print a human-readable description of them
		 * and we'll pass the count of errors back to the caller below.
		 */
		if (svclayout.nerrors() > 0) {
			subcallback();
			return;
		}

		/*
		 * The input validator ensures that there's at least one server,
		 * so by the time we get here there must be at least one
		 * availability zone.
		 */
		azs = svclayout.azs();
		assertplus.ok(azs.length > 0);
		if (typeof (args.outDirectory) != 'string') {
			/*
			 * The user specified an output stream, not a directory.
			 * This can only work when there's one AZ specified in
			 * the input.
			 */
			if (azs.length != 1) {
				subcallback(new VError('output directory ' +
				    'must be specified when generating ' +
				    'a configuration with more than one ' +
				    'availability zone'));
				return;
			}

			/*
			 * We've already checked for the ways this can fail when
			 * we checked nerrors() above.
			 */
			generated = svclayout.serialize(azs[0]);
			assertplus.string(generated);
			outstream.write(generated);
			subcallback();
			return;
		}

		/*
		 * The user specified an output directory.  We'll write one
		 * output file for each availability zone that we find.
		 */
		vasync.forEachPipeline({
		    'inputs': azs,
		    'func': function writeOneConfig(azname, pipecb) {
			var rv, outpath;

			/*
			 * As above, we've already checked the conditions that
			 * would ever cause this to fail.
			 */
			rv = svclayout.serialize(azname);
			assertplus.string(rv);
			outpath = path.join(outdir, azname);
			fs.writeFile(outpath, rv, function (err) {
				if (err) {
					err = new VError(
					    err, 'write "%s"', outpath);
				} else {
					fprintf(errstream,
					    'wrote config for "%s"\n', azname);
				}

				pipecb(err);
			});
		    }
		}, function (err) {
			if (!err && svclayout.nerrors() === 0) {
				fprintf(errstream, '\nSummary of generated ' +
				    'configuration:\n\n');
				svclayout.printSummary(errstream);
				fprintf(errstream, '\n');
			}

			subcallback(err);
		});
	    }
	], function (err) {
		if (err) {
			callback(err);
		} else {
			svclayout.printIssues(errstream);
			callback(null, svclayout.nerrors());
		}
	}));
};

/*
 * [public] Iterate zones matching a filter, invoking "callback" synchronously
 * for each one.  Returns an error if the filter was invalid.
 */
maAdm.prototype.eachZoneByFilter = function (args, callback)
{
	var byzone = null;
	var byservice = null;
	var byhost = null;
	var cnsbyhost = null;
	var i, p;

	if (args.scopeZones !== null) {
		byzone = {};
		for (i = 0; i < args.scopeZones.length; i++) {
			p = args.scopeZones[i];
			if (!this.ma_vms.hasOwnProperty(p)) {
				return (new VError(
				    'unknown zonename: %s', p));
			}

			byzone[p] = true;
		}
	}

	if (args.scopeServices !== null) {
		byservice = {};
		for (i = 0; i < args.scopeServices.length; i++) {
			p = args.scopeServices[i];
			if (svcs.serviceNameIsValid(p) == -1) {
				return (new VError(
				    'unknown service: %s', p));
			}

			byservice[p] = true;
		}
	}

	if (args.scopeComputeNodes !== null) {
		cnsbyhost = {};
		jsprim.forEachKey(this.ma_gzinfo, function (s, gzinfo) {
			cnsbyhost[gzinfo['hostname']] = s;
		});

		byhost = {};
		for (i = 0; i < args.scopeComputeNodes.length; i++) {
			p = args.scopeComputeNodes[i];
			if (cnsbyhost.hasOwnProperty(p)) {
				byhost[cnsbyhost[p]] = true;
			} else {
				if (!this.ma_gzinfo.hasOwnProperty(p)) {
					return (new VError(
					    'unknown host: %s', p));
				}

				byhost[p] = true;
			}
		}
	}

	this.ma_instances_flattened.forEach(function (row) {
		if (byzone !== null &&
		    !byzone.hasOwnProperty(row['ZONENAME'])) {
			return;
		}

		if (byservice !== null &&
		    !byservice.hasOwnProperty(row['SERVICE'])) {
			return;
		}

		if (byhost !== null &&
		    !byhost.hasOwnProperty(row['SERVER_UUID'])) {
			return;
		}

		callback(jsprim.deepCopy(row));
	});

	return (null);
};

/*
 * Returns the IP address on the "admin" network for the given component.  This
 * currently translates directly into a few NAPI calls.  It's not clear that
 * this is the most appropriate interface for extracting this information, so we
 * should be careful about generalizing its use.
 */
maAdm.prototype.findAdminIpForComponent = function (args, callback)
{
	var self = this;

	/*
	 * This is a two-step process: first we find the network_uuid for the
	 * "admin" network, and then we find the IP address of the component
	 * that the caller specified on that network.  The approach used here
	 * to find the network_uuid does not necessarily work for networks
	 * besides the SDC "admin" network, which is why we don't support that
	 * as a parameter.
	 */
	assert.ok(this.ma_instances_flattened !== null, 'must load first');
	return (vasync.waterfall([
		function fetchAdminNetworkUuid(subcallback) {
			self.ma_sdc.NAPI.listNetworks({
			    'limit': 2,
			    'name': 'admin',
			    'fabric': false
			}, function (err, networks) {
				if (err) {
					err = VError(err, 'NAPI');
				} else if (networks.length != 1) {
					err = new VError(
					    'expected 1 "admin" network, ' +
					    'found %d', networks.length);
				} else if (typeof (networks[0]['uuid']) !=
				    'string') {
					err = new VError('missing "uuid" on ' +
					    'admin network');
				}

				if (err) {
					subcallback(err);
				} else {
					subcallback(null, networks[0]['uuid']);
				}
			});
		},

		function fetchIpOnAdminNetwork(network_uuid, subcallback) {
			self.ma_sdc.NAPI.listIPs(network_uuid, {
			    'limit': 2,
			    'belongs_to_type': args['belongs_to_type'],
			    'belongs_to_uuid': args['belongs_to_uuid']
			}, function (err, addresses) {
				if (err) {
					err = new VError(err, 'NAPI');
				} else if (addresses.length != 1) {
					err = new VError(
					    'expected exactly one address, ' +
					    'but found %d\n', addresses.length);
				} else if (typeof (addresses[0]['ip']) !=
				    'string') {
					err = new VError('missing "ip" on ' +
					    'address');
				}

				if (err) {
					subcallback(err);
				} else {
					subcallback(null, addresses[0]['ip']);
				}
			});
		}
	], function (err, address) {
		callback(err, address);
	}));
};

/*
 * The dumpDeployed.* family of functions dumps either all zones
 * (dumpDeployedZones*) or a summary of distinct configurations
 * (dumpDeployedConfig.*).  Zones can be dumped organized by CN instead of by
 * service.  The configuration can also be emitted in JSON form, intended to be
 * used as input to an "update" operation.
 */

maAdm.prototype.dumpDeployedZonesByService = function (sout, conf)
{
	assert.ok(this.ma_instances_flattened !== null, 'must load first');

	var comparators, rows, colnames, columns, stream;

	comparators = [ 'SERVICE', 'SH', 'DATACENTER', 'ZONENAME' ];
	rows = common.sortObjectsByProps(
		this.ma_instances_flattened.slice(0), comparators);

	if (conf.columns)
		colnames = conf.columns;
	else if (conf.doall)
		colnames = [ 'service', 'shard', 'datacenter', 'zonename' ];
	else
		colnames = [ 'service', 'shard', 'zonename', 'gz_admin_ip' ];

	columns = colnames.map(function (colname) {
		colname = colname.toLowerCase();
		return (maColumns[colname]);
	});
	stream = new tab.TableOutputStream({
	    'columns': columns,
	    'omitHeader': conf.omitHeader,
	    'stream': sout
	});
	rows.forEach(function (r) {
		if ((!conf.filter || r['SERVICE'] == conf.filter) &&
		    (conf.doall || r['GZ HOST'] != '-'))
			stream.writeRow(r);
	});
};

maAdm.prototype.dumpDeployedZonesByCn = function (sout, conf)
{
	assert.ok(this.ma_instances !== null, 'must load deployed first');

	var self = this;
	var comparators, rows, colnames, columns, stream, last, gz, hide;

	comparators = [ 'GZ HOST', 'SERVICE', 'SH', 'ZONENAME' ];
	rows = common.sortObjectsByProps(
		this.ma_instances_flattened.slice(0), comparators);
	colnames = [ 'indent', 'service', 'shard', 'zonename' ];
	if (conf.doall)
		colnames.splice(3, 0, 'datacenter');
	columns = colnames.map(function (colname) {
		colname = colname.toLowerCase();
		return (maColumns[colname]);
	});

	hide = false;
	rows.forEach(function (row) {
		if (last !== row['GZ HOST']) {
			stream = new tab.TableOutputStream({
			    'columns': columns,
			    'stream': sout
			});

			last = row['GZ HOST'];
			gz = self.ma_gzinfo[row['SERVER_UUID']];
			if (gz) {
				fprintf(sout, 'CN %-10s %36s %-16s\n',
				    gz['hostname'], row['SERVER_UUID'],
				    gz['admin_ip']);
				hide = false;
			} else if (conf.doall) {
				fprintf(sout, 'ZONES IN OTHER DATACENTERS\n');
				hide = false;
			} else {
				hide = true;
			}
		}

		if (!hide && (!conf.filter || conf.filter == row['SERVICE']))
			stream.writeRow(row);
	});
};

maAdm.prototype.dumpDeployedConfigByService = function (sout, conf)
{
	var self, svcuuids, stream;
	var colnames, columns;

	self = this;
	var comparators = ['SH'];

	svcuuids = Object.keys(this.ma_config_bycfg).sort(function (s1, s2) {
		return (self.ma_services[s1]['name'].localeCompare(
		    self.ma_services[s2]['name']));
	});
	colnames = conf.columns || [ 'service', 'shard', 'version', 'count' ];
	columns = colnames.map(function (colname) {
		colname = colname.toLowerCase();
		return (maColumns[colname]);
	});
	stream = new tab.TableOutputStream({
	    'stream': sout,
	    'omitHeader': conf.omitHeader,
	    'columns': columns
	});

	svcuuids.forEach(function (svcid) {
		var s = self.ma_config_bycfg[svcid];

		s.eachSorted(comparators, function (row) {
			if (conf.filter && conf.filter != row['SERVICE'])
				return;

			stream.writeRow({
			    'SERVICE': self.ma_services[svcid]['name'],
			    'IMAGE': row['IMAGE'],
			    'SH': row['SH'] || '-',
			    'COUNT': row['count'],
			    'VERSION': row['VERSION']
			});
		});
	});
};

maAdm.prototype.dumpDeployedConfigByServiceJson = function (sout, conf)
{
	var self, svcuuids, rv;

	self = this;
	svcuuids = Object.keys(this.ma_config_bycn).sort(function (s1, s2) {
		return (self.ma_services[s1]['name'].localeCompare(
		    self.ma_services[s2]['name']));
	});

	rv = {};
	svcuuids.forEach(function (svcid) {
		var svcname, cnid, sc;

		svcname = self.ma_services[svcid]['name'];
		for (cnid in self.ma_config_bycn[svcid]) {
			sc = self.ma_config_bycn[svcid][cnid];

			if (!rv.hasOwnProperty(cnid)) {
				rv[cnid] = {};
			}

			assert.ok(!rv[cnid].hasOwnProperty(svcname));
			rv[cnid][svcname] = sc.summary();
		}
	});

	sout.write(JSON.stringify(rv, null, '    ') + '\n');
};

maAdm.prototype.dumpCns = function (sout, conf)
{
	var options = {};
	var colnames, columns, stream, hosts;

	colnames = conf.columns || [ 'dc', 'host', 'admin_ip', 'kind' ];
	columns = colnames.map(function (colname) {
		colname = colname.toLowerCase();
		return (maCnColumns[colname]);
	});

	if (!conf.oneachnode) {
		options = {
		    'stream': sout,
		    'omitHeader': conf.omitHeader,
		    'columns': columns
		};

		stream = new tab.TableOutputStream(options);
	} else {
		hosts = [];
	}

	var adm = this;
	var filter = conf.filter ? new RegExp(conf.filter) : null;
	jsprim.forEachKey(this.ma_cns, function (cnid, cn) {
		var gz = adm.ma_gzinfo[cnid];
		var kind;

		if (!gz)
			return;

		if (conf.onlystorage && gz && !gz['storage'])
			return;

		kind = gz['storage'] ? 'storage' : 'other';
		if (filter &&
		    (!filter.test(gz['server_uuid'])) &&
		    (!filter.test(gz['hostname'])) &&
		    (!filter.test(gz['admin_ip'])) &&
		    (!filter.test(gz['compute_id'])) &&
		    (!filter.test(kind)) &&
		    (!filter.test(gz['storage_ids'].join(','))))
			return;

		if (conf.oneachnode) {
			hosts.push(gz['hostname']);
			return;
		}

		stream.writeRow({
		    'SERVER UUID': gz['server_uuid'],
		    'HOST': gz['hostname'],
		    'DC': gz['dc'],
		    'RAM': cn['ram'],
		    'ADMIN IP': gz['admin_ip'],
		    'COMPUTE ID': gz['compute_id'] || '-',
		    'STORAGE IDS': kind == 'storage' ?
			gz['storage_ids'].sort().join(',') : '-',
		    'KIND': kind
		});
	});

	if (conf.oneachnode)
		sout.write(hosts.join(',') + '\n');
};

maAdm.prototype.dumpZkServers = function (sout, conf)
{
	var colnames, columns, options, stream;
	var zkconfig, critical, fixable;

	colnames = conf.columns ||
	    [ 'ord', 'datacenter', 'zonename', 'ip', 'port' ];

	columns = colnames.map(function (colname) {
		colname = colname.toLowerCase();
		return (maZkColumns[colname]);
	});

	options = {
	    'stream': sout,
	    'omitHeader': conf.omitHeader,
	    'columns': columns
	};

	stream = new tab.TableOutputStream(options);
	zkconfig = this.auditZkServers();
	critical = zkconfig.validationErrors.slice(0);
	fixable = zkconfig.missingInstances.map(function (i) {
		return (new VError('ZK_SERVERS[%s] has no ' +
		    'associated SAPI instance', i));
	});

	zkconfig.configuredInstances.forEach(function (zkinstance) {
		var instance;

		instance = zkinstance.instance;
		stream.writeRow({
		    '#': zkinstance.zkid,
		    'IP': zkinstance.ip,
		    'PORT': zkinstance.port,
		    'ZONENAME': instance ? instance.uuid : '-',
		    'ZONEABBR': instance ? instance.uuid.substr(0, 8) : '-',
		    'DATACENTER': instance ?
			instance.metadata['DATACENTER'] : '-'
		});
	});

	return ({
	    'critical': critical,
	    'fixable': fixable
	});
};

/*
 * [internal] Given a VM uuid, return the primary IP if we know it, or null if
 * not.
 */
maAdm.prototype.primaryIpForZone = function (uuid)
{
	var vm, ips;

	if (!this.ma_vms.hasOwnProperty(uuid))
		return (null);

	vm = this.ma_vms[uuid];
	ips = vm['nics'].filter(function (n) { return (n['primary']); });
	if (ips.length === 0)
		return (null);

	return (ips[0]['ip']);
};

/*
 * [internal] Returns the set of fields that together define a unique
 * "configuration" of a service.  This is used to figure out what instances can
 * be bucketed together as redundant instances of the same thing.  For most
 * services, the key is just the image uuid.  For postgres and moray, the shard
 * is part of the key as well.
 */
maAdm.prototype.keyForService = function (svcname)
{
	return (svcs.serviceConfigProperties(svcname));
};

/*
 * [internal] Invoked after we've loaded data into this.ma_instances to populate
 * other data structures that we'll use when creating a plan.
 */
maAdm.prototype.loadInstances = function ()
{
	assert.ok(this.ma_instances !== null);
	assert.ok(this.ma_instances_flattened === null);
	assert.ok(this.ma_config_bycn === null);
	assert.ok(this.ma_config_bycfg === null);
	assert.ok(this.ma_images !== null);

	var services, rv, svcid, i, svcname, svckey;
	var instance, metadata, server, gz, image, row, ip, version;

	services = this.ma_services;
	rv = [];
	this.ma_config_bycn = {};
	this.ma_config_bycfg = {};
	this.ma_instance_info = {};
	this.ma_instances_local_bysvcname = {};

	for (svcid in this.ma_instances) {
		svcname = services[svcid]['name'];
		svckey = svcs.serviceConfigProperties(svcname);

		this.ma_config_bycfg[svcid] =
		    new svcs.ServiceConfiguration(svckey);
		this.ma_config_bycn[svcid] = {};
		this.ma_instances_local_bysvcname[svcname] = [];

		for (i = 0; i < this.ma_instances[svcid].length; i++) {
			instance = this.ma_instances[svcid][i];
			metadata = instance['metadata'];
			if (this.ma_vms.hasOwnProperty(instance['uuid'])) {
				server = this.ma_vms[instance['uuid']]
				    ['server_uuid'];
				gz = this.ma_gzinfo[server];
				assertplus.ok(!this.ma_instance_svcname.
				    hasOwnProperty(instance['uuid']));
				this.ma_instance_svcname[
				    instance['uuid']] = svcname;
			} else {
				server = '-';
				gz = null;
			}
			image = this.ma_vms.hasOwnProperty(instance['uuid']) ?
			    this.ma_vms[instance['uuid']]['image_uuid'] : '-';
			ip = this.primaryIpForZone(instance['uuid']) || '-';
			version = this.ma_images.hasOwnProperty(image) ?
			    this.ma_images[image]['version'] : '-';

			if (gz && svcname == 'storage') {
				gz['storage'] = true;
				gz['storage_ids'].push(
				    metadata['MANTA_STORAGE_ID']);
			}

			/*
			 * The only reason to convert the shard to a string here
			 * is that in theory it should be an opaque token
			 * anyway, and the user's input will likely come in as a
			 * string, so it's easier to compare this way.
			 */
			row = {
			    'SERVICE': svcname,
			    'SH': instance['metadata']['SHARD'] ?
				instance['metadata']['SHARD'].toString() : '-',
			    'DATACENTER': metadata['DATACENTER'] || '-',
			    'ZONENAME': instance['uuid'],
			    'GZ HOST': gz ? gz['hostname'] : '-',
			    'GZ ADMIN IP': gz ? gz['admin_ip'] : '-',
			    'SERVER_UUID': server,
			    'PRIMARY IP': ip,
			    'ZONEABBR': instance['uuid'].substr(0, 8),
			    'IMAGE': image,
			    'STORAGE ID': metadata['MANTA_STORAGE_ID'] || '-',
			    'VERSION': version
			};
			rv.push(row);

			this.ma_instance_info[instance['uuid']] =
			    new instance_info.InstanceInfo({
				'uuid': instance['uuid'],
				'svcname': svcname,
				'metadata': instance['metadata'],
				'local': gz !== null,
				'server_uuid': gz !== null ?
				    server : null
			    });

			if (gz !== null) {
				this.ma_instances_local_bysvcname[
				    svcname].push(instance['uuid']);
			}

			if (image === '-')
				continue;

			this.ma_config_bycfg[svcid].incr(row);
			if (server != '-' &&
			    !this.ma_config_bycn[svcid].hasOwnProperty(server))
				this.ma_config_bycn[svcid][server] =
				    new svcs.ServiceConfiguration(svckey);
			this.ma_config_bycn[svcid][server].incr(row);
		}
	}

	this.ma_instances_flattened = rv;
};

/*
 * [internal] Invoked after we've loaded data into ma_cns to populate
 * compute node data structures that we'll use in dumping zones and
 * configurations.
 */
maAdm.prototype.loadCns = function ()
{
	assert.ok(this.ma_cns !== null);
	assert.ok(this.ma_gzinfo === null);

	var gzinfo, cnid, cn;
	var ifaces, ifacename, iface;
	var cids;

	gzinfo = {};
	for (cnid in this.ma_cns) {
		cn = this.ma_cns[cnid];
		if (cn === null)
			continue;
		gzinfo[cnid] = {
		    'dc': cn['datacenter'],
		    'hostname': cn['hostname'],
		    'server_uuid': cn['uuid'],
		    'admin_ip': 'unknown',
		    'storage': false,
		    'compute_id': null,
		    'storage_ids': []
		};
		ifaces = cn['sysinfo']['Network Interfaces'];
		for (ifacename in ifaces) {
			iface = ifaces[ifacename];
			if (iface['NIC Names'].indexOf('admin') == -1)
				continue;

			gzinfo[cnid]['admin_ip'] = iface['ip4addr'];
		}

		assertplus.ok(!this.ma_instance_svcname.hasOwnProperty(cnid));
		this.ma_instance_svcname[cnid] = 'global zone';
	}

	if (this.ma_app['metadata']) {
		cids = this.ma_app['metadata']['SERVER_COMPUTE_ID_MAPPING'];
		for (cnid in cids) {
			if (!gzinfo.hasOwnProperty(cnid))
				continue;

			gzinfo[cnid]['compute_id'] = cids[cnid];
		}
	}

	this.ma_gzinfo = gzinfo;
};

/*
 * Read a user-specified configuration describing the desired set of deployed
 * services.  We'll later construct a plan to provision, deprovision, and
 * reprovision instances to make reality match this configuration.
 */
maAdm.prototype.readConfigFromFile = function (filename, callback)
{
	var self = this;
	assert.ok(this.ma_instances_wanted === null);
	fs.readFile(filename, function (err, contents) {
		if (err) {
			callback(new VError(err, 'reading "%s":', filename));
			return;
		}

		err = self.readConfigRaw(contents.toString('utf8'));
		if (err)
			err = new VError(err, 'processing "%s"', filename);
		callback(err);
	});
};

maAdm.prototype.readConfigRaw = function (contents)
{
	var json, cnid, svcname, svckey, cfgs, sc;

	try {
		json = JSON.parse(contents);
	} catch (ex) {
		return (ex);
	}

	this.ma_instances_wanted = {};
	for (cnid in json) {
		this.ma_instances_wanted[cnid] = {};
		for (svcname in json[cnid]) {
			svckey = this.keyForService(svcname);
			this.ma_instances_wanted[cnid][svcname] = sc =
			    new svcs.ServiceConfiguration(svckey);
			cfgs = jsprim.flattenObject(
			    json[cnid][svcname], svckey.length);
			cfgs.forEach(function (c) {
				var row = {};
				svckey.forEach(function (k, i) {
					row[k] = c[i];
				});
				sc.incr(row, c[c.length - 1]);
			});
		}
	}

	var cnids = Object.keys(json);
	if (cnids.indexOf('<any>') != -1 && cnids.length > 1)
		return (new VError('cannot combine "<any>" with ' +
		    'specific compute nodes'));

	return (null);
};

/*
 * Assuming we've already loaded the current deployed configuration and the
 * user-specified configuration, generate a plan to make reality match what the
 * user wants.  If "service" is specified, then we'll only update the service
 * with name "service".  If "noreprovision" is true, we'll use provision and
 * deprovision operations even when reprovisioning would work.
 */
maAdm.prototype.generatePlan = function (callback, service, noreprovision)
{
	assert.ok(this.ma_instances_wanted !== null);
	assert.ok(this.ma_plan === null);

	var cnid, dcnconf, svcconfig, svcid, svcname, actual;
	var svcname2uuids = {};
	var usedany = false;
	var empty = new svcs.ServiceConfiguration([ 'unused' ]);
	var self = this;
	var log = this.ma_log;

	if (service && !svcs.serviceNameIsValid(service)) {
		callback(new VError('unrecognized service: "%s"', service));
		return;
	}

	log.info('generating plan');
	log.trace('generating plan', this.ma_instances_wanted,
	    this.ma_config_bycfg);
	this.ma_plan = {};

	for (svcid in this.ma_services)
		svcname2uuids[this.ma_services[svcid]['name']] = svcid;

	for (cnid in this.ma_instances_wanted) {
		if (cnid == '<any>')
			usedany = true;

		dcnconf = this.ma_instances_wanted[cnid];
		log.debug({ 'cnid': cnid }, 'user config: processing CN');
		for (svcname in dcnconf) {
			if (service && svcname !== service)
				continue;

			svcid = svcname2uuids[svcname];
			if (cnid == '<any>') {
				actual = this.ma_config_bycfg[svcid] || empty;
			} else {
				actual = this.ma_config_bycn.hasOwnProperty(
				    svcid) && this.ma_config_bycn[svcid].
				    hasOwnProperty(cnid) ?
				    this.ma_config_bycn[svcid][cnid] : empty;
			}

			/*
			 * For configurations specified in the new file,
			 * compare the desired number to what we've already got.
			 */
			dcnconf[svcname].each(function (config, key) {
				var desired_count = config['count'];
				var actual_count = actual.get(config);
				var count = desired_count - actual_count;
				log.debug({
				    'cnid': cnid,
				    'service': svcname,
				    'config': key,
				    'wanted': desired_count,
				    'have': actual_count,
				    'delta': count
				}, 'match count in new config');

				if (count > 0) {
					self.plan(cnid, svcname, key,
					    'provision', count, 'more wanted');
				} else if (count < 0) {
					self.plan(cnid, svcname, key,
					    'deprovision', -count,
					    'fewer wanted');
				}
			    });

			/*
			 * Deprovision any instances having images not specified
			 * at all in the new configuration.
			 */
			actual.each(function (config, key) {
				if (dcnconf[svcname].has(config))
					return;
				log.debug({
				    'cnid': cnid,
				    'service': svcname,
				    'config': key,
				    'delta': -config['count']
				}, 'image not present in new config');
				self.plan(cnid, svcname, key,
				    'deprovision', config['count'],
				    'image no longer used');
			});
		}

		/*
		 * Deprovision all instances of services not specified at all in
		 * the new configuration.
		 */
		for (svcid in this.ma_config_bycn) {
			svcname = this.ma_services[svcid]['name'];
			if (service && svcname !== service)
				continue;

			if (dcnconf.hasOwnProperty(svcname))
				continue;

			if (!this.ma_config_bycn.hasOwnProperty(svcid) ||
			    !this.ma_config_bycn[svcid].hasOwnProperty(cnid))
				continue;

			this.ma_config_bycn[svcid][cnid].each(
			    function (config, key) {
				log.debug({
				    'cnid': cnid,
				    'service': svcname,
				    'config': key,
				    'delta': -config['count']
				}, 'service not present in new config');

				self.plan(cnid, svcname, key,
				    'deprovision', config['count'],
				    'service no longer used');
			    });
		}
	}

	/*
	 * Deprovision everything on CNs not specified at all in the new
	 * configuration.
	 */
	if (!usedany) {
		for (svcid in this.ma_config_bycn) {
			svcname = this.ma_services[svcid]['name'];
			if (service && svcname !== service)
				continue;

			svcconfig = this.ma_config_bycn[svcid];
			for (cnid in svcconfig) {
				if (this.ma_instances_wanted.
				    hasOwnProperty(cnid))
					continue;

				svcconfig[cnid].each(function (config, key) {
					log.debug({
					    'cnid': cnid,
					    'service': svcname,
					    'config': key,
					    'delta': -config['count']
					}, 'CN not present in new config');

					self.plan(cnid, svcname, key,
					    'deprovision', config['count'],
					    'CN no longer used');
				});
			}
		}
	}

	/*
	 * Figure out in what order to execute the plan.  The plan is already
	 * divided by service (it would be nuts to update multiple services
	 * concurrently) and by compute node (because we can often safely update
	 * the same service on multiple CNs -- and sometimes need to, in the
	 * case of the marlin zones).
	 *
	 * Within those groupings, we generally want to alternate provisions and
	 * deprovisions, starting with provisions.  Otherwise, we'd end up
	 * doubling or eliminating our capacity, both of which are generally
	 * untenable.
	 *
	 * This approach also allows us to replace a "provision" + "deprovision"
	 * pair with a single "reprovision" operation, if that's allowed for
	 * this service.
	 */
	for (svcname in this.ma_plan) {
		for (cnid in this.ma_plan[svcname])
			this.ma_plan[svcname][cnid] = this.planSort(
			    svcname2uuids[svcname], cnid,
			    this.ma_plan[svcname][cnid],
			    !noreprovision && svcname !== 'marlin');
	}

	setTimeout(callback, 0);
};

/*
 * [internal] Add a deployment step to the current execution plan.
 */
maAdm.prototype.plan = function (cnid, service, configkey,
    action, count, reason)
{
	var svckey, elt;
	assert.ok(count > 0);
	if (!this.ma_plan.hasOwnProperty(service))
		this.ma_plan[service] = {};
	if (!this.ma_plan[service].hasOwnProperty(cnid))
		this.ma_plan[service][cnid] = [];

	svckey = this.keyForService(service);
	for (var i = 0; i < count; i++) {
		elt = {
		    'cnid': cnid,
		    'service': service,
		    'config': configkey,
		    'action': action,
		    'reason': reason
		};
		svckey.forEach(function (s, j) { elt[s] = configkey[j]; });
		this.ma_plan[service][cnid].push(elt);
	}
};

/*
 * [internal] Sort the operations in the current execution plan in a way that
 * will make most sense.  See planSortPartial() below for details.
 */
maAdm.prototype.planSort = function (svcid, cnid, plan, allowreprovision)
{
	/*
	 * Filter operations into buckets by the entire svckey, up to but
	 * excluding the image.
	 */
	var planbyconfig = {};
	var svcname = this.ma_services[svcid]['name'];
	var svckey = this.keyForService(svcname);
	var rv = [];
	var key;

	assert.equal(svckey[svckey.length - 1], 'IMAGE');
	svckey = svckey.slice(0, svckey.length - 1);
	plan.forEach(function (pe) {
		var rawpartialkey = svckey.map(
		    function (_, i) { return (pe['config'][i]); });
		var partialkey = JSON.stringify(rawpartialkey);
		if (!planbyconfig.hasOwnProperty(partialkey))
			planbyconfig[partialkey] = [];
		planbyconfig[partialkey].push(pe);
	});

	for (key in planbyconfig)
		rv = rv.concat(this.planSortPartial(svcid, cnid,
		    planbyconfig[key], allowreprovision));
	return (rv);
};

maAdm.prototype.planSortPartial = function (svcid, cnid, plan, allowreprovision)
{
	var provisions, deprovisions, rv;
	var dconfigs, p, d, configid, i, instance;
	var svcname, svckey, instkey, entry;

	/*
	 * Divide the operations into provisions and deprovisions so that we can
	 * stagger them (and replace each pair with reprovisions, if allowed).
	 */
	provisions = plan.filter(
	    function (pe) { return (pe['action'] == 'provision'); });
	deprovisions = plan.filter(
	    function (pe) { return (pe['action'] == 'deprovision'); });
	assert.equal(provisions.length + deprovisions.length, plan.length);

	/*
	 * Figure out how many zones we're deprovisioning for each image so that
	 * we can figure out which specific zones to deprovision.
	 */
	dconfigs = {};
	deprovisions.forEach(function (pe) {
		var key = JSON.stringify(pe['config']);
		if (!dconfigs.hasOwnProperty(key))
			dconfigs[key] = [];
		dconfigs[key].push(pe);
	});

	/*
	 * Now identify specific zones to deprovision.  This is ludicrously
	 * inefficient, but likely good enough for what we're doing.  We can
	 * preprocess the data structure to make this faster if necessary.
	 * However, it would be good to maintain the fact that this is
	 * deterministic for a given Manta configuration (because
	 * ma_instances_flattened has already been sorted) so that subsequent
	 * runs generate exactly the same plan.
	 */
	svcname = this.ma_services[svcid]['name'];
	svckey = this.keyForService(svcname);
	for (configid in dconfigs) {
		for (i = 0; dconfigs[configid].length > 0 &&
		    i < this.ma_instances_flattened.length; i++) {
			instance = this.ma_instances_flattened[i];
			instkey = svckey.map(
			    function (s) { return (instance[s]); });
			if (JSON.stringify(instkey) != configid)
				continue;

			if (cnid != '<any>' && cnid != instance['SERVER_UUID'])
				continue;

			dconfigs[configid].shift()['zonename'] =
			    instance['ZONENAME'];
		}

		assert.ok(dconfigs[configid].length === 0);
	}

	/*
	 * If allowed, translate each provision + deprovision pair into a single
	 * reprovision operation.
	 */
	rv = [];
	while (allowreprovision &&
	    provisions.length > 0 && deprovisions.length > 0) {
		p = provisions.shift();
		d = deprovisions.shift();
		assert.equal(p['cnid'], d['cnid']);
		assert.equal(p['service'], d['service']);
		assert.ok(d['zonename']);
		assert.ok(p['SH'] === d['SH']);
		entry = {
		    'cnid': p['cnid'],
		    'service': p['service'],
		    'action': 'reprovision',
		    'zonename': d['zonename'],
		    'shard': p['SH'],
		    'old_image': d['IMAGE'],
		    'new_image': p['IMAGE'],
		    'old_reason': d['reason'],
		    'new_reason': p['reason']
		};
		rv.push(entry);
	}

	/*
	 * Stagger all remaining provisions and deprovisions.
	 */
	while (provisions.length > 0 && deprovisions.length > 0) {
		p = provisions.shift();
		d = deprovisions.shift();
		assert.equal(p['cnid'], d['cnid']);
		assert.equal(p['service'], d['service']);
		assert.ok(d['zonename']);
		rv.push(p);
		rv.push(d);
	}

	while (provisions.length > 0)
		rv.push(provisions.shift());

	while (deprovisions.length > 0)
		rv.push(deprovisions.shift());

	return (rv);
};

/*
 * Execute the generated plan.  If "dryrun" is true, then just print what would
 * be done without doing it.
 */
maAdm.prototype.execPlan = function (sout, serr, dryrun, callback)
{
	var self = this;
	assert.ok(this.ma_deployer === null);
	if (!dryrun) {
		this.ma_deployer = deploy.createDeployer(this.ma_log);
		this.ma_deployer.on('error',
		    function (err) { callback(err); });
		this.ma_deployer.on('ready', function () {
			self.doExecPlan(sout, serr, dryrun, callback);
		});
	} else {
		self.doExecPlan(sout, serr, dryrun, callback);
	}
};

/*
 * [for testing only] Return a programmatically-accessible representation of the
 * plan.
 */
maAdm.prototype.dumpPlan = function ()
{
	var self = this;
	var rv = [];

	svcs.mSvcNames.forEach(function (svcname) {
		if (!self.ma_plan.hasOwnProperty(svcname))
			return;

		for (var cnid in self.ma_plan[svcname])
			rv = rv.concat(self.ma_plan[svcname][cnid].map(
			    function (p) {
				return ({
				    'cnid': p['cnid'],
				    'service': p['service'],
				    'action': p['action'],
				    'zonename': p['zonename'],
				    'image': p['IMAGE'] || p['new_image'],
				    'shard': p['SH'] || p['shard']
				});
			    }));
	});

	return (rv);
};

/*
 * See the comment in generatePlan() for why we execute the plan the way we do.
 */
maAdm.prototype.doExecPlan = function (sout, serr, dryrun, callback)
{
	var self = this;
	var count = 0;

	vasync.forEachPipeline({
	    'inputs': svcs.mSvcNames,
	    'func': function execPlanSvc(svcname, subcb) {
		var concurrency, inputs, queue, errors;

		if (!self.ma_plan.hasOwnProperty(svcname)) {
			setTimeout(subcb, 0);
			return;
		}

		count++;
		if (dryrun)
			fprintf(sout, 'service "%s"\n', svcname);

		inputs = Object.keys(self.ma_plan[svcname]);
		concurrency = dryrun ? 1 : inputs.length;
		errors = [];
		queue = vasync.queue(function execPlanSvcCn(cnid, queuecb) {
			if (dryrun)
				fprintf(sout, '  cn "%s":\n', cnid);
			self.execPlanSvcCn(sout, serr, cnid, svcname,
			    dryrun, function (err) {
				if (err)
					errors.push(err);
				queuecb();
			    });
		}, concurrency);
		queue.push(inputs);
		queue.on('end', function () {
			var err = errors.length > 0 ?
			    new MultiError(errors) : null;
			subcb(err);
		});
		queue.close();
	    }
	}, function (err) {
		if (count === 0)
			fprintf(sout, 'nothing to do\n');
		callback(err, count);
	});
};

/*
 * Execute the plan deployment-related operations for the given compute node and
 * service.
 */
maAdm.prototype.execPlanSvcCn = function (sout, serr, cnid, svcname,
    dryrun, callback)
{
	var self = this;
	var log = this.ma_log;

	vasync.forEachPipeline({
	    'inputs': self.ma_plan[svcname][cnid],
	    'func': function execPlanEntry(p, subcb) {
		if (dryrun) {
			self.execPrintAction(sout, p);
			setTimeout(subcb, 0);
			return;
		}

		if (p['action'] == 'provision') {
			var options = {};
			var k;

			if (cnid != '<any>')
				options.server_uuid = cnid;
			if (p['SH'])
				options.shard = p['SH'];
			assert(p['IMAGE'], 'image must be part of plan');
			options.image_uuid = p['IMAGE'];

			log.debug({
			    'cnid': cnid,
			    'service': svcname,
			    'params': options
			}, 'provisioning');
			fprintf(serr, 'service "%s": provisioning\n', svcname);
			for (k in options)
				fprintf(serr, '    %11s: %s\n', k, options[k]);
			self.ma_deployer.deploy(options, svcname,
			    function (err, zonename) {
				if (err) {
					log.error(err, 'deploying zone');
					subcb(err);
					return;
				}

				log.debug({
				    'cnid': cnid,
				    'service': svcname,
				    'params': options,
				    'zonename': zonename
				}, 'provisioned');
				fprintf(serr, 'service "%s": provisioned %s\n',
				    svcname, zonename);
				for (k in options)
					fprintf(serr, '    %11s: %s\n',
					    k, options[k]);
				subcb();
			    });
		} else if (p['action'] == 'deprovision') {
			log.debug({
			    'cnid': cnid,
			    'service': svcname,
			    'zone': p['zonename']
			}, 'deprovisioning');
			fprintf(serr, 'service "%s": removing %s\n',
			    svcname, p['zonename']);
			fprintf(serr, '    %11s: %s\n', 'server_uuid', cnid);
			if (p['SH'])
				fprintf(serr,
				    '    %11s: %s\n', 'shard', p['SH']);
			self.ma_deployer.undeploy(p['zonename'],
			    function (err) {
				if (err) {
					log.error(err, 'undeploying zone');
					subcb(err);
					return;
				}

				log.debug({
				    'cnid': cnid,
				    'service': svcname,
				    'zone': p['zonename']
				}, 'deprovisioned');
				fprintf(serr, 'service "%s": removed %s\n',
				    svcname, p['zonename']);
				subcb();
			    });
		} else {
			assert.equal(p['action'], 'reprovision');
			log.debug({
			    'cnid': cnid,
			    'service': svcname,
			    'zone': p['zonename'],
			    'image': p['new_image']
			}, 'reprovisioned');
			fprintf(serr, 'service "%s": reprovisioning "%s"\n',
			    svcname, p['zonename']);
			fprintf(serr, '    %11s: %s\n', 'server_uuid', cnid);
			if (p['shard'])
				fprintf(serr,
				    '    %11s: %s\n', 'shard', p['shard']);
			fprintf(serr, '    %11s: %s\n', 'new image',
			    p['new_image']);
			self.ma_deployer.reprovision(p['zonename'],
			    p['new_image'], function (err) {
				if (err) {
					log.error(err, 'reprovisioning zone');
					subcb(err);
					return;
				}

				log.debug({
				    'cnid': cnid,
				    'service': svcname,
				    'zone': p['zonename']
				}, 'reprovisioned');
				fprintf(serr, 'service "%s": reprovisioned ' +
				    '"%s"\n', svcname, p['zonename']);
				subcb();
			    });
		}
	    }
	}, callback);
};

maAdm.prototype.execPrintAction = function (sout, p)
{
	if (p['action'] == 'reprovision') {
		fprintf(sout, '    %sreprovision zone %s\n' +
		    '        (old image: %s)\n' +
		    '        (new image: %s)\n',
		    p['shard'] ? 'shard ' + p['shard'] + ': ' : '',
		    p['zonename'],
		    p['old_image'], p['new_image']);
	} else if (p['action'] == 'provision') {
		fprintf(sout, '    %sprovision (image %s)\n',
		    p['SH'] ? 'shard ' + p['SH'] + ': ' : '', p['IMAGE']);
	} else {
		assert.equal(p['action'], 'deprovision');
		fprintf(sout, '    %sdeprovision zone %s\n' +
		    '        (image: %s)\n',
		    p['SH'] ? 'shard ' + p['SH'] + ': ' : '',
		    p['zonename'], p['IMAGE']);
	}
};

maAdm.prototype.close = function ()
{
	if (this.ma_sdc === null)
		return;

	common.finiSdcClients.call(this.ma_sdc, function () {});
};

/*
 * Audits the current value of ZK_SERVERS.  This operation compares the
 * current ZK_SERVERS property value to the set of actually deployed nameservers
 * and attempts to identify misconfigurations and (optionally) correct them.
 *
 * Note: like other SAPI-related changes in the Manta deployment tooling, this
 * operation is not safe for concurrent callers!
 *
 * fetchDeployed() must have already been called to fetch the current list of
 * nameservers.
 *
 * callback() is invoked with an optional error and (if the error is null) a
 * count of changes made.
 *
 * BACKGROUND
 *
 * ZK_SERVERS is a metadata property on both "manta" and "sdc" SAPI
 * applications that describes the list of ZooKeeper servers for this SAPI
 * application.  ZooKeeper servers are used by components like registrar to
 * publish names to DNS.  They're also used by Manatee clusters to maintain
 * consensus.
 *
 * In many contexts, this array is also overloaded to denote the list of
 * nameservers for this SAPI application.  That's because they're currently
 * defined to be the same servers: each ZooKeeper instance also runs a DNS
 * server that serves a DNS subdomain backed by the ZooKeeper cluster contents.
 * That said, these don't logically have to be the same component, and future
 * consumers of this information should distinguish whether they're talking to
 * ZooKeeper or nameservers.
 *
 * Each element in the ZK_SERVERS array has these properties:
 *
 *     "num"	Ordinal number of this ZooKeeper instance.  These are
 *		allocated starting from 1 when an instance is deployed.
 *		This value is used by _all_ ZooKeeper instances when
 *		writing out their ZK configuration files.  It must be
 *		unique within the cluster.  It's unclear from the ZooKeeper
 *		documentation if it must also be consecutive and start at 1.
 *
 *		This value MUST match the ZK_ID metadata property on the
 *		corresponding SAPI instance, which is also used in ZK
 *		configuration.
 *
 *     "host"	IP address of this ZooKeeper instance.
 *
 *     "port"	Port number for this ZooKeeper instance.
 *
 * The last element in the ZK_SERVERS array must also have this property (and
 * other elements must not have this property):
 *
 *     "last"	Indicates that this is the last entry in the list.  This is used
 *		in templates for JSON configuration files to avoid including a
 *		trailing comma.
 *
 * Because ZK_SERVERS is used so directly to write out ZooKeeper configuration
 * files, this property value inherits constraints associated with ZooKeeper
 * configuration.  To avoid exposing these to operators, the only tool for
 * manipulating this property is this "fixup" function, which only makes valid
 * changes.  Changes not supported by this tool must be made using sapiadm
 * directly, which does not validate these constraints.
 */
maAdm.prototype.fixupZkServers = function (callback)
{
	var zkconfig, curservers, newservers, newmetadata;
	var self = this;

	zkconfig = this.auditZkServers();

	/*
	 * If there were any issues other than missing instances, bail out.
	 */
	if (zkconfig.validationErrors.length > 0) {
		setImmediate(callback, zkconfig.validationErrors[0]);
		return;
	}

	/*
	 * The only repair we support is removing entries that have no SAPI
	 * instances, as might happen with a failed deployment.  If there were
	 * no missing instances, then we have nothing to do.
	 */
	if (zkconfig.missingInstances.length === 0) {
		setImmediate(callback, null, zkconfig.missingInstances.length);
		return;
	}

	/*
	 * Process the missing instances in reverse order so we can splice from
	 * the given indexes (without those indexes changing as a result).
	 */
	zkconfig.missingInstances.sort(function (a, b) { return (b - a); });
	curservers = this.ma_app.metadata[maZkConfigProp];
	newservers = curservers.slice(0);
	zkconfig.missingInstances.forEach(function (i) {
		self.ma_log.debug({
		    'index': i,
		    'entry': newservers[i]
		}, 'remove ZK_SERVERS');
		newservers.splice(i, 1);
	});
	assert.equal(curservers.length,
	    newservers.length + zkconfig.missingInstances.length);

	/*
	 * Make sure "last" is set appropriately on each entry.
	 */
	newservers.forEach(function (s, i) {
		if (i == newservers.length - 1)
			s.last = true;
		else
			delete (s.last);
	});

	newmetadata = jsprim.deepCopy(this.ma_app.metadata);
	newmetadata[maZkConfigProp] = newservers;
	this.ma_sdc.SAPI.updateApplication(this.ma_app.uuid,
	    { 'metadata': newmetadata }, function (err, app) {
		if (!err)
			self.ma_app = app;
		callback(err, zkconfig.missingInstances.length);
	    });
};

/*
 * [internal] Examine the current SAPI metadata and check for problems with
 * ZK_SERVERS.  See fixupZkServers() above for information about this property.
 *
 * This function correlates elements in ZK_SERVERS with SAPI instances by
 * matching up the "num" property from ZK_SERVERS with the ZK_ID property on
 * each SAPI instance.  It identifies various completely invalid cases that
 * should never happen:
 *
 *     o more than one SAPI instance has the same ZK_ID
 *     o more than one ZK_SERVERS element has the same "num" value
 *     o the "last" property is not set correctly on elements in ZK_SERVERS
 *     o missing ZK_SERVERS element for a SAPI instance
 *     o incorrect IP address for an instance
 *     o nameservice zone removed outside of Manta's tooling (e.g. deleted via
 *       VMAPI, moved to another datacenter)
 *
 * as well as problematic cases that can happen in normal operation:
 *
 *     o missing SAPI instance for a given ZK_SERVERS element
 *       (e.g., a nameservice zone was undeployed)
 *
 * The return value is an object with:
 *
 *     validationErrors		List of Error objects describing serious
 *				validation errors like those described above.
 *				There is no support for fixing these
 *				automatically.
 *
 *     configuredInstances	List of objects describing the entries in
 *				ZK_SERVERS, each having:
 *
 *		instance		SAPI instance metadata
 *
 *		zkid			ZK_ID metadata
 *
 *		ip			IP address (from ZK_SERVERS)
 *
 *		port			PORT (from ZK_SERVERS)
 *
 *     missingInstances		List of indexes into ZK_SERVERS identifying
 *				elements with no matching SAPI instance.
 *
 *     nforeign			Number of ZK_SERVERS instances for which we have
 *				no metadata about the corresponding compute
 *				node.  This usually means that the instance is
 *				deployed inside another datacenter.
 */
maAdm.prototype.auditZkServers = function ()
{
	var instancesById, entriesByNum;
	var rv, validationErrors, missingInstances, configuredInstances;
	var nforeign;
	var zksvcid, svcid, zkservers;
	var self = this;

	if (!this.ma_app.metadata.hasOwnProperty(maZkConfigProp)) {
		return ({
		    'validationErrors': [
			new VError('%s not found in metadata', maZkConfigProp)
		    ],
		    'configuredInstances': [],
		    'missingInstances': [],
		    'nforeign': null
		});
	}

	zksvcid = null;
	for (svcid in this.ma_services) {
		if (this.ma_services[svcid]['name'] == 'nameservice') {
			zksvcid = svcid;
			break;
		}
	}
	assert.ok(zksvcid !== null, 'no "nameservice" service found');

	/*
	 * Build an index of instances by ZK_ID value.
	 */
	instancesById = {};
	validationErrors = [];
	this.ma_instances[zksvcid].forEach(function (zkinstance) {
		var uuid, zkid;

		uuid = zkinstance.uuid;
		if (!zkinstance.metadata.hasOwnProperty('ZK_ID')) {
			validationErrors.push(new VError('nameservice ' +
			    'instance "%s": no ZK_ID metadata found', uuid));
			return;
		}

		zkid = zkinstance.metadata['ZK_ID'];
		if (instancesById.hasOwnProperty(zkid)) {
			validationErrors.push(new VError('nameservice ' +
			    'instance "%s": duplicate ZK_ID "%s" (already ' +
			    'used by instance "%s"', uuid, zkid,
			    instancesById[zkid].uuid));
			return;
		}

		instancesById[zkid] = zkinstance;
	});

	/*
	 * Build an index of ZK_SERVERS by "num" value.
	 */
	entriesByNum = {};
	zkservers = this.ma_app.metadata[maZkConfigProp];
	zkservers.forEach(function (zkserver, i) {
		var label, islast;

		label = 'ZK_SERVERS[' + i + ']';
		islast = (i == zkservers.length - 1);
		if (islast) {
			if (zkserver.last !== true) {
				validationErrors.push(new VError(
				    '%s: expected "last" field', label));
			}
		} else {
			if (zkserver.last) {
				validationErrors.push(new VError(
				    '%s: expected no "last" field', label));
			}
		}

		if (entriesByNum.hasOwnProperty(zkserver.num)) {
			validationErrors.push(new VError(
			    '%s: duplicate num "%s"', label, zkserver.num));
			return;
		}

		entriesByNum[zkserver.num] = zkserver;
	});

	/*
	 * Look for SAPI instances with missing or incorrect ZK_SERVERS
	 * metadata.  There are no known cases where this would normally happen,
	 * so we treat this as a validation error that we won't try to repair.
	 */
	nforeign = 0;
	jsprim.forEachKey(instancesById, function (zkid, zkinstance) {
		var uuid, serverUuid, ip, entry;

		uuid = zkinstance.uuid;
		if (!entriesByNum.hasOwnProperty(zkid)) {
			validationErrors.push(new VError(
			    'nameservice instance "%s": missing ZK_SERVERS ' +
			    'entry', uuid));
			return;
		}

		serverUuid = zkinstance.params['server_uuid'];
		if (!self.ma_vms.hasOwnProperty(uuid)) {
			if (self.ma_cns[serverUuid] !== null) {
				validationErrors.push(new VError(
				    'nameservice instance "%s": VM appears ' +
				    'to have been provisioned in this ' +
				    'datacenter, but could not be found in ' +
				    'VMAPI', uuid));
				return;
			}
			nforeign++;
			return;
		}

		ip = self.primaryIpForZone(uuid);
		if (ip === null) {
			validationErrors.push(new VError(
			    'nameservice instance "%s": failed to find ' +
			    'instance\'s primary IP address', uuid));
			return;
		}

		entry = entriesByNum[zkid];
		if (ip != entry.host) {
			validationErrors.push(new VError(
			    'nameservice instance "%s": primary IP "%s" ' +
			    'does not match ZK_SERVERS metadata IP "%s"',
			    uuid, ip, entry.host));
		}
	});

	missingInstances = [];
	configuredInstances = zkservers.map(function (zkserver) {
		return ({
		    'instance': instancesById[zkserver.num] || null,
		    'zkid': zkserver.num,
		    'ip': zkserver.host,
		    'port': zkserver.port
		});
	});

	rv = {
	    'configuredInstances': configuredInstances,
	    'validationErrors': validationErrors,
	    'missingInstances': missingInstances,
	    'nforeign': nforeign
	};

	/*
	 * Check for ZK_SERVERS metadata entries that have no SAPI instance.
	 */
	zkservers.forEach(function (zkserver, i) {
		var zkid = zkserver.num;
		if (instancesById.hasOwnProperty(zkid))
			return;

		missingInstances.push(i);
	});

	return (rv);
};

function fmtListDateTime(ts)
{
	if (ts === null) {
		return ('-');
	}

	return (new Date(ts).toISOString());
}

function fmtDateOnly(ts)
{
	if (ts === null) {
		return ('-');
	}

	return (new Date(ts).toISOString().substr(0, '2017-02-06'.length));
}

/*
 * TODO This implementation is copied from manta-marlin.  It should be moved to
 * node-jsprim.
 */
function fmtDuration(ms)
{
	var hour, min, sec, rv;

	/* compute totals in each unit */
	assertplus.number(ms, 'ms');
	sec = Math.floor(ms / 1000);
	min = Math.floor(sec / 60);
	hour = Math.floor(min / 60);

	/* compute offsets for each unit */
	ms %= 1000;
	sec %= 60;
	min %= 60;

	rv = '';
	if (hour > 0)
		rv += hour + 'h';

	if (hour > 0 || min > 0) {
		if (hour > 0 && min < 10)
			rv += '0' + min + 'm';
		else
			rv += min + 'm';
	}

	if ((hour > 0 || min > 0) && sec < 10)
		rv += '0' + sec;
	else
		rv += sec;

	rv += 's';
	return (rv);
}

function formatCmdOutput(prefix, streamname, str)
{
	var trimmed;

	/*
	 * Don't bother printing empty outputs.
	 */
	trimmed = str.trim();
	if (trimmed.length === 0) {
		return ('');
	}

	if (trimmed.indexOf('\n') == -1) {
		return (extsprintf.sprintf('%s%6s:           %s\n',
		    prefix, streamname, JSON.stringify(str)));
	}

	return (extsprintf.sprintf('%s%6s:\n%s\n',
	    prefix, streamname, prependLines(str, '        | ')));
}

function prependLines(str, prefix)
{
	var lines, i;

	/*
	 * A simple "map" would be concise and elegant, but wouldn't handle the
	 * trailing newline case very well.
	 */
	lines = str.split('\n');
	for (i = 0; i < lines.length - 1; i++) {
		lines[i] = prefix + lines[i];
	}

	if (lines.length > 0 && lines[lines.length - 1].length > 0) {
		lines[lines.length - 1] = prefix + lines[lines.length - 1];
	}

	return (lines.join('\n'));
}
