/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/adm.js: library interface to the "manta-adm" functionality
 */

var assert = require('assert');
var fs = require('fs');
var jsprim = require('jsprim');
var sprintf = require('sprintf-js').sprintf;
var tab = require('tab');
var vasync = require('vasync');
var VError = require('verror').VError;
var MultiError = require('verror').MultiError;
var common = require('../lib/common');
var deploy = require('../lib/deploy');

var maMaxConcurrency = 50; /* concurrent requests to SDC services */

/* Public interface */
exports.columnNames = columnNames;
exports.cnColumnNames = cnColumnNames;
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
	'width': 14
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
	'width': 8
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
	'width': 8
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

/*
 * Service names, in the order they get deployed.
 */
var maSvcNames = [
    'nameservice',
    'postgres',
    'moray',
    'electric-moray',
    'storage',
    'authcache',
    'webapi',
    'loadbalancer',
    'jobsupervisor',
    'jobpuller',
    'medusa',
    'ops',
    'marlin'
];


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
 *       or more of the dumpDeployed* family of funcitons.
 *
 *     o "update" operation: call loadSdcConfig, then readConfigFromFile, then
 *       fetchDeployed, then generatePlan, then some combination of execPlan
 *       with "dryrun" options.
 *
 * Any other sequence of operations (skipping any of these, or duplicating those
 * the ones that can't explicitly be called more than once) is invalid.
 */
function maAdm(log)
{
	this.ma_sdc = null;
	this.ma_app = null;
	this.ma_services = null;
	this.ma_instances = null;
	this.ma_instances_flattened = null;
	this.ma_cns = null;
	this.ma_vms = null;
	this.ma_gzinfo = null;
	this.ma_plan = null;
	this.ma_instances_wanted = null;
	this.ma_appname = 'manta';
	this.ma_log = log;
	this.ma_id = 0;
	this.ma_reqs = {};
	this.ma_recent = [];
	this.ma_recent_limit = 30;
	this.ma_deployer = null;
	this.ma_svc_keys = {
	    'postgres': [ 'SH', 'IMAGE' ],
	    'moray': [ 'SH', 'IMAGE' ]
	};
	this.ma_svc_key_default = [ 'IMAGE' ];

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

maAdm.prototype.dumpConfigCoal = function (sout)
{
	return (this.dumpConfigCommon({
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
	}, sout));
};

maAdm.prototype.dumpConfigLab = function (sout)
{
	return (this.dumpConfigCommon({
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
	    'marlin': 10
	}, sout));
};

maAdm.prototype.dumpConfigCommon = function (conf, sout)
{
	var self = this;
	var config, imagesbysvcname;
	var svcid, svc;
	var nwarnings = 0;

	imagesbysvcname = {};
	for (svcid in this.ma_services) {
		svc = this.ma_services[svcid];
		imagesbysvcname[svc['name']] = svc['params']['image_uuid'];
	}

	config = {};
	maSvcNames.forEach(function (svcname) {
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
		if (self.ma_svc_keys.hasOwnProperty(svcname)) {
			conf['shards'].forEach(function (shard) {
				config[svcname][shard] = {};
				config[svcname][shard][image] = conf[svcname];
			});
		} else {
			config[svcname][image] = conf[svcname];
		}
	});

	sout.write(JSON.stringify({ '<any>': config }, null, '    ') + '\n');
	return (nwarnings);
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
			sc.each(function (row, rowkey) {
				insert(rv, row['count'],
				    [ cnid, svcname ].concat(rowkey));
			});
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

/*
 * [internal] Returns the set of fields that together define a unique
 * "configuration" of a service.  This is used to figure out what instances can
 * be bucketed together as redundant instances of the same thing.  For most
 * services, the key is just the image uuid.  For postgres and moray, the shard
 * is part of the key as well.
 */
maAdm.prototype.keyForService = function (svcname)
{
	return (this.ma_svc_keys[svcname] || this.ma_svc_key_default);
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
	var instance, metadata, server, gz, image, row, ips, ip;

	services = this.ma_services;
	rv = [];
	this.ma_config_bycn = {};
	this.ma_config_bycfg = {};

	for (svcid in this.ma_instances) {
		svcname = services[svcid]['name'];
		svckey = this.ma_svc_keys[svcname] || this.ma_svc_key_default;

		this.ma_config_bycfg[svcid] = new ServiceConfiguration(svckey);
		this.ma_config_bycn[svcid] = {};

		for (i = 0; i < this.ma_instances[svcid].length; i++) {
			instance = this.ma_instances[svcid][i];
			metadata = instance['metadata'];
			server = instance['params']['server_uuid'];
			gz = this.ma_gzinfo[server];
			image = this.ma_vms.hasOwnProperty(instance['uuid']) ?
			    this.ma_vms[instance['uuid']]['image_uuid'] : '-';
			ips = this.ma_vms.hasOwnProperty(instance['uuid']) ?
			    this.ma_vms[instance['uuid']]['nics'].filter(
			    function (n) { return (n['primary']); }) : [];
			ip = ips.length > 0 ? ips[0]['ip'] : '-';

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
				    new ServiceConfiguration(svckey);
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
			    new ServiceConfiguration(svckey);
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
	var empty = new ServiceConfiguration([ 'unused' ]);
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

	maSvcNames.forEach(function (svcname) {
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
	    'inputs': maSvcNames,
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
 * A "ServiceConfiguration" describes a count of service instances matching
 * one or more unique configurations for that service.  For most services, this
 * is just a count of instances by image id.  Services like "postgres" and
 * "moray" also have a shard number.  In general, we don't expect to have many
 * configurations for a given service, so we store this as a flat array.
 */
function ServiceConfiguration(keys)
{
	assert(keys.length > 0);
	this.sc_keys = keys;
	this.sc_counts = [];
}

ServiceConfiguration.prototype.each = function (callback)
{
	var self = this;
	this.sc_counts.forEach(function (config) {
		callback(config,
		    self.sc_keys.map(function (k) { return (config[k]); }));
	});
};

ServiceConfiguration.prototype.get = function (config)
{
	var i, k;
	var key, row;
	for (i = 0; i < this.sc_counts.length; i++) {
		row = this.sc_counts[i];
		for (k = 0; k < this.sc_keys.length; k++) {
			key = this.sc_keys[k];
			if (config[key] != row[key])
				break;
		}

		if (k == this.sc_keys.length)
			return (row['count']);
	}

	return (0);
};

ServiceConfiguration.prototype.has = function (config)
{
	var i, k;
	var key, row;
	for (i = 0; i < this.sc_counts.length; i++) {
		row = this.sc_counts[i];
		for (k = 0; k < this.sc_keys.length; k++) {
			key = this.sc_keys[k];
			if (config[key] != row[key])
				break;
		}

		if (k == this.sc_keys.length)
			return (true);
	}

	return (false);
};

ServiceConfiguration.prototype.incr = function (config, count)
{
	var i, k;
	var key, row;

	if (arguments.length == 1)
		count = 1;

	for (i = 0; i < this.sc_counts.length; i++) {
		row = this.sc_counts[i];
		for (k = 0; k < this.sc_keys.length; k++) {
			key = this.sc_keys[k];
			if (config[key] != row[key])
				break;
		}

		if (k == this.sc_keys.length) {
			row['count'] += count;
			return;
		}
	}

	var obj = jsprim.deepCopy(config);
	obj['count'] = count;
	this.sc_counts.push(obj);
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

function insert(obj, value, keys)
{
	assert(keys.length > 0);
	if (keys.length == 1) {
		obj[keys[0]] = value;
		return;
	}

	if (!obj.hasOwnProperty(keys[0]))
		obj[keys[0]] = {};
	insert(obj[keys[0]], value, keys.slice(1));
}
