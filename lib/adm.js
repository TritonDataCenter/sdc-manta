/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/adm.js: library interface to the "manta-adm" functionality
 */

var assert = require('assert');
var assertplus = require('assert-plus');
var fs = require('fs');
var jsprim = require('jsprim');
var net = require('net');
var sprintf = require('sprintf-js').sprintf;
var tab = require('tab');
var vasync = require('vasync');
var VError = require('verror').VError;
var MultiError = require('verror').MultiError;
var common = require('../lib/common');
var deploy = require('../lib/deploy');
var layout = require('./layout');
var svcs = require('./services');

var maMaxConcurrency = 50; /* concurrent requests to SDC services */

/*
 * ZooKeeper configuration property.  This can be overridden for testing.
 */
var maZkConfigProp = process.env['ZK_SERVERS_PROPNAME'] || 'ZK_SERVERS';

/* Public interface (used only inside this module) */
exports.columnNames = columnNames;
exports.cnColumnNames = cnColumnNames;
exports.zkColumnNames = zkColumnNames;
exports.MantaAdm = maAdm;

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
    }
};

function columnNames()
{
	return (Object.keys(maColumns).filter(function (c) {
		return (maColumns[c]['label'] !== '');
	}));
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
 * There are three supported use cases:
 *
 *     o "genconfig" operation: call loadSdcConfig, then fetchDeployed, then
 *       dumpConfigCoal or dumpConfigLab.
 *
 *     o "show" operation: call loadSdcConfig, then fetchDeployed, then then one
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
	 * CNAPI server objects, indexed by server_uuid.
	 */
	this.ma_cns = null;

	/*
	 * VMAPI vm objects, indexed by instance uuid.
	 */
	this.ma_vms = null;

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
		 * XXX want a way to fetch all application instances from VMAPI.
		 * We currently use owner_uuid as a proxy for that, but that's
		 * not necessarily correct.
		 */
		function fetchVmInfo(_, stepcb) {
			var params = {
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
					cns[instance.params.server_uuid] = null;
				}
			}

			self.fetchCnInfo(Object.keys(cns), stepcb);
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

	this.loadCns();
	this.loadInstances();
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
 * that file.  See manta-adm(1).  This function behaves like dumpConfigCoal()
 * and dumpConfigLab() in that it dumps a JSON form of the configuration to
 * the given stream and invokes the callback.
 */
maAdm.prototype.genconfigFromFile = function (args, callback)
{
	var images, filename, outstream, errstream;

	assertplus.object(args, 'args');
	assertplus.string(args.filename, 'args.filename');
	assertplus.object(args.outstream, 'args.outstream');
	assertplus.object(args.errstream, 'args.errstream');

	images = this.latestImagesByService();
	filename = args.filename;
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
		var svclayout;

		svclayout = layout.generateLayout({
		    'dcconfig': dcconfig,
		    'images': images
		});

		svclayout.serialize(outstream, errstream);
		subcallback(null, svclayout.nerrors());
	    }
	], function (err, nissues) {
		if (err) {
			callback(err);
		} else {
			callback(null, nissues);
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
	rows = sortObjectsByProps(this.ma_instances_flattened.slice(0),
	    comparators);

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

	comparators = [ 'GZ HOST', 'SERVICE', 'SHARD', 'ZONENAME' ];
	rows = sortObjectsByProps(this.ma_instances_flattened.slice(0),
	    comparators);
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
	svcuuids = Object.keys(this.ma_config_bycfg).sort(function (s1, s2) {
		return (self.ma_services[s1]['name'].localeCompare(
		    self.ma_services[s2]['name']));
	});
	colnames = conf.columns || [ 'service', 'shard', 'image', 'count' ];
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
		s.each(function (row) {
			if (conf.filter && conf.filter != row['SERVICE'])
				return;

			stream.writeRow({
			    'SERVICE': self.ma_services[svcid]['name'],
			    'IMAGE': row['IMAGE'],
			    'SH': row['SH'] || '-',
			    'COUNT': row['count']
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

	var services, rv, svcid, i, svcname, svckey;
	var instance, metadata, server, gz, image, row, ip;

	services = this.ma_services;
	rv = [];
	this.ma_config_bycn = {};
	this.ma_config_bycfg = {};

	for (svcid in this.ma_instances) {
		svcname = services[svcid]['name'];
		svckey = svcs.serviceConfigProperties(svcname);

		this.ma_config_bycfg[svcid] =
		    new svcs.ServiceConfiguration(svckey);
		this.ma_config_bycn[svcid] = {};

		for (i = 0; i < this.ma_instances[svcid].length; i++) {
			instance = this.ma_instances[svcid][i];
			metadata = instance['metadata'];
			server = instance['params']['server_uuid'];
			gz = this.ma_gzinfo[server];
			image = this.ma_vms.hasOwnProperty(instance['uuid']) ?
			    this.ma_vms[instance['uuid']]['image_uuid'] : '-';
			ip = this.primaryIpForZone(instance['uuid']) || '-';

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
			    'STORAGE ID': metadata['MANTA_STORAGE_ID'] || '-'
			};
			rv.push(row);

			if (image === '-')
				continue;

			this.ma_config_bycfg[svcid].incr(row);
			if (!this.ma_config_bycn[svcid].hasOwnProperty(server))
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
 *      	allocated starting from 1 when an instance is deployed.
 *      	This value is used by _all_ ZooKeeper instances when
 *      	writing out their ZK configuration files.  It must be
 *      	unique within the cluster.  It's unclear from the ZooKeeper
 *      	documentation if it must also be consecutive and start at 1.
 *
 *      	This value MUST match the ZK_ID metadata property on the
 *      	corresponding SAPI instance, which is also used in ZK
 *      	configuration.
 *
 *     "host"	IP address of this ZooKeeper instance.
 *
 *     "port"	Port number for this ZooKeeper instance.
 *
 * The last element in the ZK_SERVERS array must also have this property (and
 * other elements must not have this property):
 *
 *     "last"	Indicates that this is the last entry in the list.  This is used
 *     		in templates for JSON configuration files to avoid including a
 *     		trailing comma.
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
 *
 * as well as problematic cases that can happen in normal operation:
 *
 *     o missing SAPI instance for a given ZK_SERVERS element
 *       (e.g., a nameservice zone was undeployed)
 *
 * The return value is an object with:
 *
 *     validationErrors		List of Error objects describing serious
 *     				validation errors like those described above.
 *     				There is no support for fixing these
 *     				automatically.
 *
 *     configuredInstances	List of objects describing the entries in
 *     				ZK_SERVERS, each having:
 *
 *     		instance		SAPI instance metadata
 *
 *		zkid			ZK_ID metadata
 *
 *		ip			IP address (from ZK_SERVERS)
 *
 *		port			PORT (from ZK_SERVERS)
 *
 *     missingInstances		List of indexes into ZK_SERVERS identifying
 *     				elements with no matching SAPI instance.
 *
 *     nforeign			Number of ZK_SERVERS instances for which we have
 *     				no metadata about the corresponding compute
 *     				node.  This usually means that the instance is
 *     				deployed inside another datacenter.
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
		if (self.ma_cns[serverUuid] === null) {
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


/*
 * Utility functions (which could be moved to a common file)
 */

function fprintf(stream)
{
	var args = Array.prototype.slice.call(arguments, 1);
	var msg = sprintf.apply(null, args);
	stream.write(msg);
}

function sortObjectsByProps(rows, comparators)
{
	return (rows.sort(function (i1, i2) {
		var c, comp;
		var v1, v2, rv;

		for (c = 0; c < comparators.length; c++) {
			comp = comparators[c];
			v1 = i1[comp];
			v2 = i2[comp];
			rv = typeof (v1) == 'string' ?
			    v1.localeCompare(v2) : v1 - v2;
			if (rv !== 0)
				return (rv);
		}

		return (0);
	}));
}
