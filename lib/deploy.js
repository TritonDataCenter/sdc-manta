/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/deploy.js: common functions for deploying instances of Manta zones
 */

var assert = require('assert-plus');
var async = require('async');
var fs = require('fs');
var node_uuid = require('node-uuid');
var path = require('path');
var util = require('util');
var vasync = require('vasync');
var sprintf = util.format;
var common = require('./common');
var mod_ssl = require('./ssl');
var EventEmitter = require('events').EventEmitter;
var VError = require('verror').VError;

exports.deploy = deploy;
exports.createDeployer = createDeployer;

/*
 * Storage zone deployments cannot be done concurrently, so we funnel all
 * storage zone deployments through a vasync Queue with concurrency 1.  This is
 * global to the process.  Obviously, even that's not sufficient when there are
 * multiple processes involved, but it helps in the important case of using
 * manta-adm to deploy multiple storage zones.  See MANTA-2185 for details.
 */
var dStorageQueue = vasync.queue(
    function (func, callback) { func(callback); }, 1);

/*
 * Deploy a new instance of a Manta service.  This is a one-shot method that
 * creates a Deployer and then deploys a zone.  If you're deploying more than
 * one zone, you're better off creating your own deployer and then calling
 * "deploy" as many times as you want.  Arguments:
 *
 *     options		an object with optional properties:
 *
 *         networks	array of network names (strings) that this zone should
 *         		be provisioned with
 *
 *         server_uuid	server uuid (string) on which to provision this zone
 *
 *     svcname		the friendly name of the service to be deployed
 *     			(e.g., "nameservice", "loadbalancer", "moray", etc.)
 *
 *     log		a bunyan logger
 *
 *     callback		invoked upon completion as callback([err])
 */
function deploy(options, svcname, ilog, callback)
{
	var deployer = createDeployer(ilog);
	deployer.on('error', function (err) { callback(err); });
	deployer.on('ready', function () {
		deployer.deploy(options, svcname, callback);
	});
}

/*
 * Creates a new Deployer, which can be used to deploy several Manta zones.
 * This operation initializes connections to various SDC services and emits
 * "ready" when ready, or "error" if something goes wrong.
 */
function createDeployer(log)
{
	return (new Deployer(log));
}

/*
 * A single Deployer instance basically just keeps its own connections to
 * various SDC services and a cached copy of the "Manta" and "SDC" applications.
 * For consumers that want to deploy several zones, this is more efficient than
 * reinitializing those connections each time.
 */
function Deployer(ilog)
{
	var self = this;
	self.log = ilog;

	EventEmitter.call(this);

	async.waterfall([
		function initClients(cb) {
			var log = self.log;
			log.info('initing sdc clients');
			common.initSdcClients.call(self, cb);
		},

		function getPoseidon(cb) {
			var log = self.log;
			log.info('getting poseidon user');
			getUser(self, 'poseidon', function (err, user) {
				self.poseidon = user;
				return (cb(err));
			});
		},

		function loadSdcApplication(cb) {
			var sapi = self.SAPI;
			var log = self.log;
			var search_opts = { 'name': 'sdc' };
			log.info('finding "sdc" application');
			sapi.listApplications(search_opts,
			    function (err, apps) {
				if (err) {
					log.error(err,
					    'failed to list applications');
					return (cb(err));
				}

				if (apps.length === 0) {
					var msg = 'application "sdc" not found';
					log.error(msg);
					return (cb(new Error(msg)));
				}

				self.sdc_app = apps[0];
				return (cb(null));
			    });
		},

		function getMantaApplication(cb) {
			var log = self.log;
			log.info('finding "manta" application');
			common.getMantaApplication.call(self,
			    self.poseidon.uuid, function (err, app) {
				if (err)
					return (cb(err));

				if (!app) {
					var msg =
					    'application "manta" not found';
					log.error(msg);
					return (cb(new Error(msg)));
				}

				self.manta_app = app;
				return (cb());
			});
		},

		function getMantaServices(cb) {
			var log, params;
			log = self.log;
			params = {
			    'include_master': true,
			    'application_uuid': self.manta_app['uuid']
			};
			log.info(params,
			    'fetching "manta" application services');
			self.SAPI.listServices(params, function (err, svcs) {
				if (err) {
					cb(err);
					return;
				}

				self.services = svcs;
				cb();
			});
		},

		function checkShardConfigs(cb) {
			var log = self.log;
			var app = self.manta_app;
			var md = app.metadata;
			var missing = [];
			var message, err;

			log.info('checking shard configuration parameters');

			if (typeof (md[common.MARLIN_SHARD]) != 'string' ||
			    md[common.MARLIN_SHARD].length === 0) {
				missing.push(common.MARLIN_SHARD);
			}

			if (typeof (md[common.STORAGE_SHARD]) != 'string' ||
			    md[common.STORAGE_SHARD].length === 0) {
				missing.push(common.STORAGE_SHARD);
			}

			if (!Array.isArray(md[common.INDEX_SHARDS]) ||
			    md[common.INDEX_SHARDS].length === 0) {
				missing.push(common.INDEX_SHARDS);
			}

			if (missing.length === 0) {
				setImmediate(cb);
				return;
			}

			message = 'cannot deploy zones before shards have ' +
			    'been configured (see manta-shardadm)\n';
			message += 'details: metadata properties missing or ' +
			    'not valid: ' + missing.join(', ');
			err = new Error(message);
			log.error(err);
			setImmediate(cb, err);
		},

		function checkHashRingConfig(cb) {
			var log = self.log;
			var app = self.manta_app;
			var md = app.metadata;
			var message, err;

			log.info('checking shard configuration parameters');

			if (typeof (md[common.HASH_RING_IMAGE]) != 'string' ||
			    md[common.HASH_RING_IMAGE].length === 0 ||
			    typeof (md[common.HASH_RING_IMGAPI_SERVICE]) !=
			    'string' ||
			    md[common.HASH_RING_IMGAPI_SERVICE].length === 0) {
				message = 'cannot deploy zones before hash ' +
				    'ring topology has been created ' +
				    '(see manta-create-topology.sh)';
				err = new Error(message);
				log.error(err);
				setImmediate(cb, err);
			} else {
				setImmediate(cb);
			}
		}
	], function (err) {
		if (err)
			self.emit('error', err);
		else
			self.emit('ready');
	});
}

util.inherits(Deployer, EventEmitter);

Deployer.prototype.close = function (cb)
{
	common.finiSdcClients.call(this, cb);
};

/*
 * Actually deploy a Manta service zone for service "svcname".  For argument
 * details, see deploy() above.
 */
Deployer.prototype.deploy = function (options, svcname, callback)
{
	var self, allservices;

	self = {};
	for (var k in this)
		self[k] = this[k];

	self.options = options;
	self.zone_uuid = node_uuid.v4();
	self.svcname = svcname;
	allservices = this.services;

	async.waterfall([
		function getMantaService(cb) {
			var log = self.log;
			var svcs = allservices.filter(
			    function (s) { return (s['name'] == svcname); });
			if (svcs.length < 1) {
				var t = 'Service "%s" not found.  ' +
				    'Did you run manta-init?  If so, ' +
				    'is it a valid service?';
				var message = sprintf(t, self.svcname);
				var e = new Error(message);
				e.message = message;
				log.error(message);
				return (cb(e));
			}

			self.service = svcs[0];
			log.debug({ svc: self.service },
			    'found %s service', self.svcname);
			return (cb(null));
		},

		function ensureZk(cb) {
			var app = self.manta_app;
			var log = self.log;

			if (self.svcname === 'nameservice') {
				return (cb(null));
			}

			log.info('ensuring ZK servers have been deployed');

			if (!app.metadata || !app.metadata['ZK_SERVERS'] ||
			    app.metadata['ZK_SERVERS'].length < 1) {
				var message = 'zk servers missing or empty ' +
				    'in the manta application.  Has the ' +
				    'nameservice been deployed yet?';
				log.error({
					zkServers: app.metadata['ZK_SERVERS']
				}, message);
				var e = new Error(message);
				e.message = message;
				return (cb(e));
			}
			return (cb(null));
		},

		function generateSSLCertificate(cb) {
			var log = self.log;
			var sapi = self.SAPI;
			var app = self.manta_app;
			var svc = self.service;

			if (svc.name !== 'loadbalancer') {
				log.info('service "%s" doesn\'t need an ' +
				    'SSL certificate', svc.name);
				return (cb(null));
			}

			if (svc.metadata['SSL_CERTIFICATE']) {
				log.info('SSL certificate already present');
				return (cb(null));
			}

			log.info('generating an ssl certificate');

			var file = sprintf('/tmp/cert.%d', process.pid);
			var svc_name = app.metadata['MANTA_SERVICE'];

			async.waterfall([
				function (subcb) {
					mod_ssl.generateCertificate.call(self,
					    file, svc_name, subcb);
				},
				function (subcb) {
					fs.readFile(file, 'ascii',
					    function (err, contents) {
						if (err) {
							log.error(err,
							    'failed to ' +
							    'read SSL cert');
						} else {
							log.debug(
							    'read SSL cert');
						}

						fs.unlink(file, function (_) {
							return (subcb(
							    err, contents));
						});
					});
				},
				function (cert, subcb) {
					assert.string(cert, 'cert');
					assert.func(subcb, 'subcb');

					var opts = {};
					opts.metadata = {};
					opts.metadata['SSL_CERTIFICATE'] = cert;

					sapi.updateService(svc.uuid, opts,
					    function (err) {
						if (err) {
							log.error(err,
							    'failed to ' +
							    'save SSL cert');
							return (subcb(err));
						}

						log.debug('saved SSL cert');
						return (subcb(null));
					});
				}
			], cb);
		},

		function reserveIP(cb) {
			if (self.svcname !== 'nameservice')
				return (cb(null, {}));

			// XXX I can really do this after it's deployed, no need
			// to reserve before provisioning.
			var log = self.log;
			log.info('reserving nic');
			reserveAndGetNic(self, 'manta', self.zone_uuid,
			    self.poseidon.uuid,
			    function (err, nic) {
				self.nic = nic;
				cb(err, nic);
			    });
		},

		function updateZKServers(nic, cb) {
			var sapi = self.SAPI;
			var log = self.log;

			if (self.svcname !== 'nameservice')
				return (cb(null));

			log.info('updating the list of zk servers in the ' +
			    'sapi manta application');

			assert.object(nic, 'nic');
			assert.string(nic.ip, 'nic.ip');

			var metadata = self.manta_app.metadata;

			if (!metadata)
				metadata = {};
			if (!metadata.ZK_SERVERS)
				metadata.ZK_SERVERS = [];

			metadata.ZK_SERVERS.push({
				host: nic.ip,
				port: 2181
			});

			var len = metadata.ZK_SERVERS.length;

			metadata.ZK_SERVERS[len - 1].num = len;

			for (var ii = 0; ii < len - 1; ii++)
				delete metadata.ZK_SERVERS[ii].last;
			metadata.ZK_SERVERS[len - 1].last = true;

			var opts = {};
			opts.metadata = metadata;

			sapi.updateApplication(self.manta_app.uuid, opts,
			    function (err, app) {
				if (!err)
					self.manta_app = app;
				return (cb(err));
			});
		},

		function ensureComputeId(cb) {
			if (self.svcname !== 'storage') {
				return (cb(null));
			}

			var log = self.log;
			var serverUuid;

			log.debug('Ensuring that the server has a compute id');

			function getComputeId() {
				log.debug({
					serverUuid: serverUuid
				}, 'server uuid for looking up compute id');
				var m = 'Error getting compute id';
				common.getOrCreateComputeId.call(
					self, serverUuid, function (err, cid) {
						if (err) {
							return (cb(err));
						}

						if (!cid) {
							var e = new Error(m);
							e.message = m;
							return (cb(e));
						}
						log.debug({
							computeId: cid
						}, 'found compute id');
						return (cb(null));
					});
			}

			if (self.options.server_uuid) {
				serverUuid = self.options.server_uuid;
				getComputeId();
			} else {
				common.findServerUuid.call(
					self, function (err, id) {
						if (err) {
							return (cb(err));
						}
						serverUuid = id;
						getComputeId();
					});
			}

		},

		function deployMantaInstance(cb) {
			createInstance.call(null, self,
			    self.manta_app, self.service, function (err, inst) {
				if (err)
					return (cb(err));
				self.instance = inst;
				return (cb(null));
			});
		},

		function configureMarlinComputeZone(cb) {
			var cnapi = self.CNAPI;
			var vmapi = self.VMAPI;
			var log = self.log;
			var params = { 'uuid': self.instance.uuid };

			if (self.svcname !== 'marlin')
				return (cb(null));

			log.info('configuring compute zone, ' +
			    'getting vmapi object');
			vmapi.getVm(params, function (err, vm) {
				if (err) {
					log.error(err, 'failed to get zone ' +
					    '"%s" after instance created',
					    params.uuid);
					return (cb(err));
				}

				var server = vm.server_uuid;
				var script = sprintf(
				    '%s/tools/mrdeploycompute %s',
				    common.MARLIN_DIR, params.uuid);
				log.info({
					server: server,
					script: script
				}, 'running script to configure compute zone');

				cnapi.commandExecute(server, script,
				    function (suberr) {
					if (suberr) {
						log.error(suberr, 'failed to ' +
						    'configure compute zone %s',
						    params.uuid);
						return (cb(err));
					}

					log.info('configured compute zone %s',
					    params.uuid);
					return (cb(null));
				    });
			});
		}
	], function (err) {
		callback(err, self.zone_uuid);
	});
};

/*
 * Undeploy a SAPI instance.
 */
Deployer.prototype.undeploy = function (instance, callback)
{
	var self = this;
	var svcname, cnid;

	async.waterfall([
		function getInstanceType(cb) {
			self.log.info('fetching SAPI instance', instance);
			self.SAPI.getInstance(instance, function (err, inst) {
				var svcs;

				if (!err) {
					svcs = self.services.filter(
					    function (s) {
						return (s['uuid'] ==
						    inst['service_uuid']);
					    });

					if (svcs.length === 0) {
						err = new VError(
						    'zone "%s" has ' +
						    'unexpected service "%s"',
						    instance,
						    inst['service_uuid']);
					} else {
						svcname = svcs[0]['name'];
					}
				}

				cb(err);
			});
		},

		function getVmInfo(cb) {
			var params = { 'uuid': instance };
			self.log.info(params, 'fetching VMAPI details');
			self.VMAPI.getVm(params, function (err, vm) {
				if (err) {
					cb(new VError(err,
					    'failed to get "%s" from VMAPI',
					    instance));
					return;
				}

				cnid = vm['server_uuid'];
				cb();
			});
		},

		function rmMarlinZone(cb) {
			if (svcname != 'marlin') {
				cb();
				return;
			}

			var log = self.log;
			var scriptpath = sprintf('%s/tools/mrzoneremove %s',
			    common.MARLIN_DIR, instance);
			log.info({
			    'server': cnid,
			    'script': scriptpath
			}, 'running script to remove compute zone');

			self.CNAPI.commandExecute(cnid, scriptpath,
			    function (err) {
				if (err) {
					err = new VError(err,
					    'failed to remove compute zone ' +
					    '"%s"', instance);
					log.error(err);
					cb(err);
					return;
				}

				log.info('removed compute zone "%s"', instance);
				cb();
			    });
		},

		function sapiDelete(cb) {
			self.log.info('deleting SAPI instance', instance);
			self.SAPI.deleteInstance(instance, cb);
		}
	], function (err) {
		self.log.info('undeploy complete', instance);
		callback(err);
	});
};

/*
 * Reprovision a SAPI instance.
 */
Deployer.prototype.reprovision = function (instance, image_uuid, callback)
{
	this.SAPI.reprovisionInstance(instance, image_uuid, callback);
};


// -- User management

function getUser(self, login, cb) {
	var ufds = self.UFDS;
	var log = self.log;

	assert.string(login, 'login');

	ufds.getUser(login, function (err, ret) {
		if (err)
			log.error(err, 'failed to get %s', login);
		return (cb(err, ret));
	});
}


// -- Network management

function reserveAndGetNic(self, name, zone_uuid, owner_uuid, cb) {
	var log = self.log;
	var napi = self.NAPI;

	assert.string(name, 'name');
	assert.string(zone_uuid, 'zone_uuid');
	assert.string(owner_uuid, 'owner_uuid');

	var opts = {
		belongs_to_uuid: zone_uuid,
		owner_uuid: owner_uuid,
		belongs_to_type: 'zone'
	};

	log.info({ opts: opts }, 'provisioning NIC');

	async.waterfall([
		function (subcb) {
			napi.listNetworks({ name: name },
			    function (err, networks) {
				if (err) {
					log.error(err,
					    'failed to list networks');
					return (subcb(err));
				}

				log.debug({ network: networks[0] },
				    'found network %s', name);

				return (subcb(null, networks[0].uuid));
			});
		},
		function (network_uuid, subcb) {
			napi.provisionNic(network_uuid, opts,
			    function (err, nic) {
				if (err) {
					log.error(err,
					    'failed to provision NIC');
					return (cb(err));
				}

				log.info({ nic: nic }, 'provisioned NIC');

				return (subcb(null, nic));
			});
		}
	], cb);
}


// -- SAPI functions

function createInstance(self, app, svc, cb) {
	var sapi = self.SAPI;
	var log = self.log;

	assert.string(self.config.datacenter_name,
		    'self.config.datacenter_name');

	assert.object(app, 'app');
	assert.object(app.metadata, 'app.metadata');
	assert.string(app.metadata.REGION, 'app.metadata.REGION');
	assert.string(app.metadata.DNS_DOMAIN, 'app.metadata.DNS_DOMAIN');

	assert.object(svc, 'svc');
	assert.string(svc.name, 'svc.name');
	assert.string(svc.uuid, 'svc.uuid');

	var inst_uuid = self.zone_uuid ? self.zone_uuid : node_uuid.v4();

	var params = {};

	/*
	 * Traditionally we've used numeric shards (e.g. 1.moray, 2.moray, etc.)
	 * but there's no reason they have to be numbers.  We could have
	 * 1-marlin.moray, marlin.moray, or anything similar.
	 */
	var shard = '1';
	if (self.options.shard)
		shard = self.options.shard;

	/*
	 * The root of all service hostnames is formed from the application's
	 * region and DNS domain.
	 */
	var service_root = sprintf('%s.%s',
	    app.metadata.REGION, app.metadata.DNS_DOMAIN);
	var service_name = sprintf('%s.%s', self.svcname, service_root);

	params.alias = service_name + '-' + inst_uuid.substr(0, 8);


	/*
	 * Prefix with the shard for things that are shardable...
	 */
	if (['postgres', 'moray'].indexOf(self.svcname) !== -1) {
		params.alias = shard + '.' + params.alias;
	}

	params.tags = {};
	params.tags.manta_role = svc.name;

	if (self.options.server_uuid)
		params.server_uuid = self.options.server_uuid;

	if (self.options.image_uuid)
		params.image_uuid = self.options.image_uuid;

	if (self.options.networks) {
		var networks = [];
		self.options.networks.forEach(function (token) {
			networks.push({ uuid: token });
		});
		params.networks = networks;
	}

	var metadata = {};
	metadata.DATACENTER = self.config.datacenter_name;
	metadata.SERVICE_NAME = service_name;
	metadata.SHARD = shard;

	if (self.svcname === 'nameservice') {
		var len = 1;
		if (app.metadata.ZK_SERVERS)
			len = app.metadata.ZK_SERVERS.length;
		metadata.ZK_ID = len;
	}

	if (self.svcname === 'postgres') {
		metadata.SERVICE_NAME = sprintf('%s.moray.%s',
		    shard, service_root);
		metadata.MANATEE_SHARD_PATH = sprintf('/manatee/%s',
		    metadata.SERVICE_NAME);
	}

	if (self.svcname === 'moray') {
		metadata.SERVICE_NAME = sprintf('%s.moray.%s',
		    shard, service_root);
	}

	if (self.svcname === 'storage') {
		metadata.SERVICE_NAME = sprintf('stor.%s', service_root);
	}

	if (self.svcname === 'webapi' || self.svcname === 'loadbalancer')
		metadata.SERVICE_NAME = app.metadata['MANTA_SERVICE'];

	if (self.svcname === 'marlin')
		params.tags.manta_role = 'compute';

	/*
	 * This zone should get its configuration the local (i.e. same
	 * datacenter) SAPI instance, as well as use the local UFDS instance.
	 */
	var config = self.config;
	metadata['SAPI_URL'] = config.sapi.url;
	metadata['UFDS_URL'] = config.ufds.url;
	metadata['UFDS_ROOT_DN'] = config.ufds.bindDN;
	metadata['UFDS_ROOT_PW'] = config.ufds.bindPassword;
	metadata['SDC_NAMESERVERS'] = self.sdc_app.metadata.ZK_SERVERS;

	var queuecb;

	async.waterfall([
		function (subcb) {
			if (svc.name !== 'storage')
				return (subcb(null));

			log.debug('putting "storage" zone provision for ' +
			    '"%s" into the queue', inst_uuid);
			dStorageQueue.push(function (_queuecb) {
				/*
				 * When we reach here, we're the only "storage"
				 * zone deployment that's going on right now.
				 * Save the queue callback so that we can invoke
				 * it when we finish deploying to free up the
				 * queue for someone else.
				 */
				queuecb = _queuecb;
				log.debug('dequeueing "storage" zone ' +
				    'provision for "%s"', inst_uuid);
				subcb();
			});
		},
		function (subcb) {
			if (svc.name !== 'storage')
				return (subcb(null));

			/*
			 * The manta_storage_id should be the next available
			 * number.
			 */
			var opts = {};
			opts.service_uuid = svc.uuid;
			opts.include_master = true;

			log.info('finding next manta_storage_id');

			sapi.listInstances(opts, function (err, insts) {
				if (err) {
					log.error(err, 'failed to list ' +
					    'storage instances');
					return (subcb(err));
				}

				/*
				 * Find the highest-numbered storage id and pick
				 * the next one.
				 */
				var mStorageId = pickNextStorageId(
				    insts, metadata.SERVICE_NAME);
				if (mStorageId instanceof Error) {
					log.error(err);
					return (subcb(err));
				}

				metadata.MANTA_STORAGE_ID = mStorageId;
				params.tags.manta_storage_id = mStorageId;
				subcb();
			});
		},
		function (subcb) {
			log.info('locating user script');

			var file = sprintf('%s/../scripts/user-script.sh',
			    path.dirname(__filename));
			file = path.resolve(file);

			fs.readFile(file, 'ascii', function (err, contents) {
				if (err && err['code'] == 'ENOENT') {
					log.debug('no user script');
				} else if (err) {
					log.error(err,
					    'failed to read user script');
					return (subcb(err));
				} else {
					metadata['user-script'] = contents;
					log.debug('read user script from %s',
					    file);
				}

				return (subcb(null));
			});
		},
		function (subcb) {
			var opts = {};
			opts.params = params;
			opts.metadata = metadata;
			opts.uuid = inst_uuid;
			opts.master = true;

			log.info({ opts: opts }, 'creating instance');

			sapi.createInstance(svc.uuid, opts,
			    function (err, inst) {
				if (err) {
					log.error(err, 'failed to create ' +
					    'instance');
					return (subcb(err));
				}

				log.info({ inst: inst }, 'created instance');

				return (subcb(null, inst));
			});
		}
	], function () {
		if (queuecb) {
			log.debug('done with "storage" zone ' +
			    'provision for "%s"', inst_uuid);
			setTimeout(queuecb, 0);
		}

		cb.apply(null, Array.prototype.slice.call(arguments));
	});
}

/*
 * Given a list of SAPI instances for storage nodes, return an unused Manta
 * storage id.  If we're at all unsure, we return an error rather than
 * potentially returning a conflicting name.
 */
function pickNextStorageId(instances, svcname)
{
	var max, inst, instname, numpart;
	var i, p, n;
	var err = null;

	max = 0;
	for (i = 0; i < instances.length; i++) {
		inst = instances[i];
		instname = inst.metadata.MANTA_STORAGE_ID;

		if (typeof (instname) != 'string') {
			err = new VError('instance "%s": missing or ' +
			    'invalid MANTA_STORAGE_ID metadata', inst.uuid);
			break;
		}

		p = instname.indexOf('.' + svcname);
		if (p == -1 || p === 0) {
			err = new VError('instance "%s": instance name ' +
			    '("%s") does not contain expected suffix (".%s")',
			    inst.uuid, instname, svcname);
			break;
		}

		numpart = instname.substr(0, p);
		n = parseInt(numpart, 10);
		if (isNaN(n) || n < 1) {
			err = new VError('instance "%s": instance name ' +
			    '("%s") does not start with a positive integer',
			    inst.uuid, instname);
			break;
		}

		max = Math.max(max, n);
	}

	if (err !== null) {
		return (new VError(err,
		    'failed to allocate MANTA_STORAGE_ID'));
	}

	return (sprintf('%d.%s', max + 1, svcname));
}
