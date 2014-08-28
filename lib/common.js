/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/common.js: a garbage barge of common routines for use in the manta tools
 */

var assert = require('assert-plus');
var fs = require('fs');
var path = require('path');
var sdc = require('sdc-clients');
var vasync = require('vasync');
var VError = require('verror').VError;

var exec = require('child_process').exec;
var sprintf = require('util').format;

var Logger = require('bunyan');


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

function initLogger(filename) {
	var __file = path.basename(filename, '.js');
	var log = new Logger({
		name: __file,
		streams: [
			{
				level: 'debug',
				path: path.join(LOG_DIR, __file + '.log')
			}
		],
		serializers: Logger.stdSerializers
	});

	return (log);
}

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

function getOrCreateComputeId(server_uuid, cb) {
	var self = this;

	var app = self.application || self.manta_app;
	var sapi = self.SAPI;
	var log = self.log;
	var message;
	var e;

	if (!app.metadata['SERVER_COMPUTE_ID_MAPPING']) {
		message = 'No SERVER_COMPUTE_ID_MAPPING in manta ' +
			'application';
		e = new Error(message);
		e.message = message;
		log.error({
			'application': app
		}, message);
		return (cb(e));
	}
	var mapping = app.metadata['SERVER_COMPUTE_ID_MAPPING'];
	var compute_id = mapping[server_uuid];
	if (compute_id) {
		return (cb(null, compute_id));
	}

	// Create a new compute id...
	var next_available = 1;
	Object.keys(mapping).forEach(function (uuid) {
		var cid = mapping[uuid];
		var idx = parseInt(cid.substr(
			0, cid.indexOf('.')), 10);
		next_available = Math.max(next_available, idx + 1);
	});
	compute_id = sprintf('%d.cn.%s', next_available,
			     app.metadata['DOMAIN_NAME']);

	// TODO: We really need etag support in sapi (SAPI-136)
	getMantaApplication.call(self, function (err, ao) {
		mapping = ao.metadata['SERVER_COMPUTE_ID_MAPPING'];

		log.debug({
			server_uuid: server_uuid,
			compute_id: compute_id,
			mapping: mapping
		}, 'updating mapping with new compute id for server');

		mapping[server_uuid] = compute_id;
		app.metadata['SERVER_COMPUTE_ID_MAPPING'] = mapping;
		var update = {
			'metadata': {
				'SERVER_COMPUTE_ID_MAPPING': mapping
			}
		};
		sapi.updateApplication(app.uuid, update, function (err2) {
			return (cb(err2, compute_id));
		});
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

exports.shuffle = shuffle;
exports.domainToPath = domainToPath;
exports.initLogger = initLogger;
exports.initSdcClients = initSdcClients;
exports.finiSdcClients = finiSdcClients;
exports.getMantaApplication = getMantaApplication;
exports.runOnEachMarlinNode = runOnEachMarlinNode;
exports.confirm = confirm;
exports.findServerUuid = findServerUuid;
exports.getOrCreateComputeId = getOrCreateComputeId;
exports.commandExecute = commandExecute;
