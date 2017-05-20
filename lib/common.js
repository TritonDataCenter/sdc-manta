/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/common.js: a garbage barge of common routines for use in the manta tools
 */

var assert = require('assert-plus');
var fs = require('fs');
var path = require('path');
var restifyClients = require('restify-clients');
var sdc = require('sdc-clients');
var vasync = require('vasync');
var VError = require('verror').VError;

var exec = require('child_process').exec;
var sprintf = require('util').format;


// -- Global variables

var ROOT_DIR = path.resolve(__dirname, '..');

exports.SSH_DIR = path.join(ROOT_DIR, 'etc');
exports.SSH_KEY_NAME = 'poseidon_id_rsa';

exports.SSL_DIR = path.join(ROOT_DIR, 'etc');
exports.SSL_CERT_NAME = 'ssl.pem';

var LOG_DIR = exports.LOG_DIR = path.join(ROOT_DIR, 'log');

exports.MARLIN_DIR = '/opt/smartdc/agents/lib/node_modules/marlin';

exports.MARLIN_SHARD = 'MARLIN_MORAY_SHARD';
exports.STORAGE_SHARD = 'STORAGE_MORAY_SHARD';
exports.INDEX_SHARDS = 'INDEX_MORAY_SHARDS';
exports.HASH_RING_IMAGE = 'HASH_RING_IMAGE';
exports.HASH_RING_IMGAPI_SERVICE = 'HASH_RING_IMGAPI_SERVICE';

exports.CONFIG_FILE_DEFAULT = path.join(__dirname, '..', 'etc', 'config.json');


// -- Helper functions

function rand(limit) {
	return (Math.floor(Math.random() * limit));
}

/*
 * The USB key may store more than one administrative IP for a service.  If so,
 * split the IPs and choose one randomly.
 */
function chooseIP(ips) {
	var tokens = ips.trim().split(' ');
	return (tokens[rand(tokens.length)]);
}

// Fisher-Yates shuffle
// http://sedition.com/perl/javascript-fy.html
function shuffle(arr) {
	if (arr.length === 0)
		return (arr);

	var i = arr.length;
	while (--i > 0) {
		var j = Math.floor(Math.random() * (i + 1));
		var tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;
	}

	return (arr);
}

// domainToPath(1.moray.manta.joyent.com) => /com/joyent/manta/moray/1
function domainToPath(domain) {
	assert.string(domain, 'domain');
	return ('/' + domain.split('.').reverse().join('/'));
}

/*
 * Note that this function instantiates clients configured according to a config
 * file relative to the root of this repository.  Some commands accept the
 * configuration file as a command-line option, but those are not respected
 * here.  This function should be parametrized.
 */
function initSdcClients(cb) {
	var self = this;

	var file = path.join(path.dirname(__filename), '../etc/config.json');

	fs.readFile(file, function (err, contents) {
		if (err)
			throw (err);

		var config = JSON.parse(contents);

		self.CNAPI = new sdc.CNAPI({
			log: self.log,
			url: config.cnapi.url,
			agent: false
		});

		self.VMAPI = new sdc.VMAPI({
			log: self.log,
			url: config.vmapi.url,
			agent: false
		});

		self.IMGAPI = new sdc.IMGAPI({
			log: self.log,
			url: config.imgapi.url,
			agent: false
		});

		self.REMOTE_IMGAPI = new sdc.IMGAPI({
			log: self.log,
			url: config.remote_imgapi.url,
			agent: false
		});

		self.NAPI = new sdc.NAPI({
			log: self.log,
			url: config.napi.url,
			agent: false
		});

		self.SAPI = new sdc.SAPI({
			log: self.log,
			url: config.sapi.url,
			agent: false
		});

		self.AMON = new sdc.Amon({
			log: self.log,
			url: config.amon.url,
			agent: false
		});

		self.AMON_RAW = restifyClients.createJsonClient({
			log: self.log,
			url: config.amon.url,
			agent: false
		});

		self.UFDS = new sdc.UFDS({
			log: self.log,
			url: config.ufds.url,
			bindDN: config.ufds.bindDN,
			bindPassword: config.ufds.bindPassword,
			retry: {
			    retries: 3
			},
			cache: {
				size: 1000,
				expiry: 300
			}
		});

		self.config = config;

		var errhandler = function (connerr) {
			cb(new VError(connerr, 'failed to connect to UFDS'));
		};
		self.UFDS.on('error', errhandler);
		self.UFDS.once('connect', function () {
			self.log.info('UFDS: connected');

			/*
			 * We currently don't handle subsequent errors from
			 * UFDS.  Recall that the client already reconnects
			 * automatically, so these would be more serious errors.
			 */
			self.UFDS.removeListener('error', errhandler);
			cb();
		});

	});
}

function finiSdcClients(cb) {
	this.CNAPI.close();
	this.VMAPI.close();
	this.IMGAPI.close();
	this.REMOTE_IMGAPI.close();
	this.NAPI.close();
	this.SAPI.close();
	this.AMON.close();
	this.AMON_RAW.close();
	this.UFDS.close(cb);
}

function getMantaApplication(owner_uuid, cb) {
	if (typeof (owner_uuid) === 'function') {
		cb = owner_uuid;
		owner_uuid = undefined;
	}
	var sapi = this.SAPI;
	var log = this.log;

	var search = {};
	search.name = 'manta';
	if (owner_uuid) {
		search.owner_uuid = owner_uuid;
	}

	search.include_master = true;

	sapi.listApplications(search, function (err, apps) {
		if (err) {
			log.error(err, 'failed to list applications');
			return (cb(err));
		}

		log.debug({ app: apps[0] }, 'found manta application');

		assert.ok(apps.length <= 1);

		return (cb(null, apps[0]));
	});
}

function findServerUuid(cb) {
	var self = this;
	var log = self.log;
	var app = self.application || self.manta_app;
	var cnapi = self.CNAPI;
	var message;
	var e;

	if (app.metadata.SIZE === 'production') {
		message = 'Not finding server_uuid for production deployments';
		e = new Error(message);
		e.message = message;
		return (cb(e));
	}

	cnapi.listServers(function (err, servers) {
		if (err) {
			log.error(err, 'failed to list servers');
			return (cb(err));
		}

		if (servers.length !== 1) {
			message = 'findServerUuid failed, invalid number of ' +
				'servers in cnapi';
			e = new Error(message);
			e.message = message;
			log.error({
				'servers': servers
			}, message);
			return (cb(e));
		}

		return (cb(null, servers[0].uuid));
	});
}

/*
 * Compute IDs are strings of the form "N.cn.DOMAIN", where N is a positive
 * integer; e.g., "1.cn.manta.joyent.us".
 */
function parseComputeId(idstr) {
	assert.string(idstr, 'idstr');

	var m = idstr.match(/^([0-9]+)\.cn\.(.+)$/);
	if (!m) {
		return (null);
	}

	var id = parseInt(m[1], 10);
	if (isNaN(id) || id < 1) {
		return (null);
	}

	return ({
		id: id,
		domain: m[2]
	});
}

/*
 * Read the SAPI Application for Manta and extract the mapping from Server UUID
 * to Compute ID.  Fail if this object does not exist in the Application object
 * already; it should have been created during initialisation.
 */
function cid_getServerComputeIdMapping(cid, next) {
	assert.object(cid.cid_self, 'cid.cid_self');

	var log = cid.cid_self.log;

	getMantaApplication.call(cid.cid_self, function (err, ao) {
		if (err) {
			next(new VError(err, 'could not load manta ' +
			    'application from SAPI'));
			return;
		}

		if (!ao.metadata['SERVER_COMPUTE_ID_MAPPING']) {
			next(new VError('"SERVER_COMPUTE_ID_MAPPING" ' +
			    'not found in manta application in SAPI'));
			return;
		}

		assert.strictEqual(cid.cid_mapping, null);
		cid.cid_mapping = ao.metadata['SERVER_COMPUTE_ID_MAPPING'];

		log.debug({
			mapping: cid.cid_mapping
		}, 'read compute ID mapping from SAPI');

		next();
	});
}

/*
 * Process the mapping object.  If there is already a mapping for this server,
 * return it.  Otherwise, create a new mapping with the next available number
 * after the highest number currently in use.
 */
function cid_ensureMappingExists(cid, next) {
	assert.object(cid.cid_self, 'cid.cid_self');
	assert.object(cid.cid_mapping, 'cid.cid_mapping');
	assert.string(cid.cid_server_uuid, 'cid.cid_server_uuid');

	var log = cid.cid_self.log;
	var app = cid.cid_self.application || cid.cid_self.manta_app;
	var domain = app.metadata['DOMAIN_NAME'];

	/*
	 * Walk the set of existing mappings prior to checking if this server
	 * already has one.  In addition to determining the next available
	 * compute ID, this loop also checks the structure of the existing
	 * mapping (e.g. for duplicates, malformed IDs, etc).
	 */
	var next_available = 1;
	var seen = {};
	var servers = Object.keys(cid.cid_mapping);
	for (var i = 0; i < servers.length; i++) {
		var uuid = servers[i];
		var compute_id = cid.cid_mapping[uuid];
		var parsed = parseComputeId(compute_id);

		if (seen[compute_id] !== undefined) {
			/*
			 * Although this should never happen, there has been at
			 * least one bug in the Manta setup tooling that could
			 * have induced this condition (cf. MANTA-2789).
			 * Unfortunately there is no automatic corrective
			 * action we can take here; the operator will need to
			 * do some manual cleanup.
			 */
			next(new VError('multiple servers found with compute ' +
			    'ID "%s": "%s" and "%s"', compute_id, uuid,
			    seen[compute_id]));
			return;
		}
		seen[compute_id] = uuid;

		if (parsed === null || parsed.domain !== domain) {
			next(new VError('found invalid compute ID mapping ' +
			    'for server "%s": "%s"', uuid, compute_id));
			return;
		}

		next_available = Math.max(next_available, parsed.id + 1);
	}
	assert.ok(!isNaN(next_available) && next_available >= 1);

	/*
	 * Now that we have checked the integrity of the existing mapping, we
	 * know it is safe to use an existing assignment if it exists:
	 */
	if (cid.cid_mapping[cid.cid_server_uuid]) {
		log.debug({
			compute_id: cid.cid_mapping[cid.cid_server_uuid],
			server_uuid: cid.cid_server_uuid
		}, 'using existing compute ID mapping for server');

		next();
		return;
	}

	/*
	 * Assign a new Compute ID to this Server:
	 */
	var new_compute_id = sprintf('%d.cn.%s', next_available, domain);

	log.info({
		new_compute_id: new_compute_id,
		server_uuid: cid.cid_server_uuid,
		existing_mappings: cid.cid_mapping
	}, 'assigning new compute ID mapping for server');

	cid.cid_mapping[cid.cid_server_uuid] = new_compute_id;
	cid.cid_flush = true;

	next();
}

/*
 * If we updated the mapping, it must be written back to SAPI before we
 * return it to the caller.
 */
function cid_flushToSAPI(cid, next) {
	if (!cid.cid_flush) {
		next();
		return;
	}

	var app = cid.cid_self.application || cid.cid_self.manta_app;
	var sapi = cid.cid_self.SAPI;
	var log = cid.cid_self.log;

	log.debug({
		application_uuid: app.uuid,
		server_uuid: cid.cid_server_uuid,
		mapping: cid.cid_mapping
	}, 'updating mapping with new compute id for server');

	assert.object(cid.cid_mapping);
	assert.string(cid.cid_mapping[cid.cid_server_uuid]);
	var update = {
		'metadata': {
			'SERVER_COMPUTE_ID_MAPPING': cid.cid_mapping
		}
	};

	sapi.updateApplication(app.uuid, update, function (err) {
		if (err) {
			next(new VError(err, 'failed to update manta ' +
			    'application in SAPI'));
			return;
		}

		/*
		 * Now that we have successfully updated SAPI, update the
		 * cached copy of the application metadata:
		 */
		app.metadata['SERVER_COMPUTE_ID_MAPPING'] = cid.cid_mapping;
		next();
	});
}

var SERVER_COMPUTE_ID_MAPPING_QUEUE = vasync.queuev({
	worker: function (cid, next) {
		vasync.pipeline({
			funcs: [
				cid_getServerComputeIdMapping,
				cid_ensureMappingExists,
				cid_flushToSAPI
			],
			arg: cid
		}, next);
	},
	concurrency: 1
});

function getOrCreateComputeId(server_uuid, cb) {
	var cid = {
		cid_self: this,
		cid_server_uuid: server_uuid,
		cid_mapping: null,
		cid_flush: false
	};

	/*
	 * We serialise the operation of reading (and possibly creating)
	 * mappings from Server to Compute ID.  Provided the operator does not
	 * invoke multiple copies of "manta-adm update" concurrently,
	 * this serialisation ensures that the IDs are unique.
	 */
	SERVER_COMPUTE_ID_MAPPING_QUEUE.push(cid, function (err) {
		if (err) {
			cb(new VError(err, 'could not get or create ' +
			    'server to compute ID mapping (server %s)',
			    server_uuid));
			return;
		}

		assert.string(cid.cid_mapping[server_uuid]);
		cb(null, cid.cid_mapping[server_uuid]);
	});
}

// WARNING: This currently runs on *all* cns.  Only run this if it won't make
// a mess.
function runOnEachMarlinNode(script, cb) {
	var self = this;
	var cnapi = self.CNAPI;
	var log = self.log;

	// TODO: Shouldn't be done on *all* servers, but only the ones that
	// are for manta.  See: MANTA-2047
	cnapi.listServers(function (err, servers) {
		if (err) {
			log.error(err, 'failed to list servers');
			return (cb(err));
		}

		log.debug('listed %d servers', servers.length);

		var uuids = servers.map(function (s) {
			return (s.uuid);
		});

		vasync.forEachParallel({
			func: function (uuid, subcb) {
				commandExecute.call(self,
						    uuid, script, subcb);
			},
			inputs: uuids
		}, cb);
	});
}

function commandExecute(server_uuid, script, cb) {
	var self = this;
	var cnapi = self.CNAPI;
	var log = self.log;


	log.info({
		server_uuid: server_uuid,
		script: script
	}, 'running script on remote host');

	cnapi.commandExecute(server_uuid, script, function (err) {
		if (err) {
			log.error(err,
				'failed to execute command on %s',
				server_uuid);
			return (cb(err));
		}

		log.info('executed command on %s', server_uuid);
		return (cb(null));
	});
}

function confirm(msg, cb)
{
	process.stdout.write(msg);
	process.stdin.resume();
	process.stdin.setEncoding('utf8');
	process.stdin.setRawMode(true);
	var answer = '';
	process.stdin.on('data', function (ch) {
		ch = ch + '';

		switch (ch) {
		case '\n':
		case '\r':
		case '\u0004':
			// They've finished typing their answer
			process.stdin.setRawMode(false);
			process.stdin.pause();
			process.stdin.write('\n');
			cb(answer == 'y');
			break;
		case '\u0003':
			// Ctrl C
			cb(false);
			break;
		default:
			// More plaintext characters
			process.stdout.write(ch);
			answer += ch;
			break;
		}
	});
}

/*
 * This is the inverse of jsprim's pluck() function, and should probably move
 * into jsprim.
 */
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

/*
 * Given a list of arrays, produce a single array with the elements from each
 * array taken in round-robin fashion.  So given arrays A, B, and C, this
 * produces [ A[0], B[0], C[0], A[1], B[1], C[1], A[2], ... ].  If any array is
 * shorter than the others, that array is skipped once all of its elements have
 * been used.  For example:
 *
 *     stripe([ [ 1, 2, 3 ], [ 4, 5, 6, 7, 8 ] ])
 *
 * returns
 *
 *     [ 1, 4, 2, 5, 3, 6, 7, 8 ]
 */
function stripe(lists)
{
	var nextelt, rv, li, rvlength;

	assert.arrayOfArray(lists, 'lists');
	nextelt = new Array(lists.length);
	rvlength = 0;
	for (li = 0; li < lists.length; li++) {
		nextelt[li] = 0;
		rvlength += lists[li].length;
	}

	if (lists.length === 0) {
		return ([]);
	}

	rv = [];
	for (li = 0; rv.length < rvlength; li = (li + 1) % lists.length) {
		if (nextelt[li] >= lists[li].length) {
			continue;
		}

		rv.push(lists[li][nextelt[li]++]);
	}

	return (rv);
}

/*
 * Utility function to sort objects for dumping
 * by service configuration or compute node
 */
function sortObjectsByProps(rows, comparators)
{
	return (rows.sort(function (i1, i2) {
		var c, comp;
		var v1, v2, rv;
		for (c = 0; c < comparators.length; c++) {
			comp = comparators[c];
			v1 = i1[comp];
			v2 = i2[comp];
			assert.ok(typeof (v1) == typeof (v2));
			rv = (typeof (v1) == 'string' && isNaN(v1)) ?
			    v1.localeCompare(v2) : v1 - v2;
			if (rv !== 0)
				return (rv);
		}
	}));
}

exports.shuffle = shuffle;
exports.domainToPath = domainToPath;
exports.initSdcClients = initSdcClients;
exports.finiSdcClients = finiSdcClients;
exports.getMantaApplication = getMantaApplication;
exports.runOnEachMarlinNode = runOnEachMarlinNode;
exports.confirm = confirm;
exports.findServerUuid = findServerUuid;
exports.getOrCreateComputeId = getOrCreateComputeId;
exports.commandExecute = commandExecute;
exports.insert = insert;
exports.stripe = stripe;
exports.sortObjectsByProps = sortObjectsByProps;
