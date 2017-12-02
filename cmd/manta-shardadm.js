#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * manta-shardadm.js: CLI tool for SAPI
 */

var assert = require('assert-plus');
var async = require('async');
var cmdln = require('cmdln');
var common = require('../lib/common');
var cp = require('child_process');
var fs = require('fs');
var path = require('path');
var sdc = require('sdc-clients');
var sprintf = require('sprintf-js').sprintf;
var util = require('util');

var Cmdln = cmdln.Cmdln;
var Logger = require('bunyan');


function Shardadm() {
	Cmdln.call(this, {
		name: 'manta-shardadm',
		desc: 'Manage manta shards'
	});
}
util.inherits(Shardadm, Cmdln);

Shardadm.prototype.init = function (opts, args, cb) {
	assert.object(opts, 'opts');
	assert.object(args, 'args');
	assert.func(cb, 'cb');

	this.log = new Logger({
		name: __filename,
		serializers: Logger.stdSerializers,
		streams: [ {
			level: 'debug',
			path: '/var/log/manta-shardadm.log'
		} ]
	});

	var CFG = path.resolve(__dirname, '../etc/config.json');
	var config = JSON.parse(fs.readFileSync(CFG, 'utf8'));

	this.client = new sdc.SAPI({
		url: config.sapi.url,
		log: this.log,
		agent: false
	});

	Cmdln.prototype.init.apply(this, arguments);
};

Shardadm.prototype.do_list = function (subcmd, opts, args, cb) {
	var fmt = '%-12s %-28s %s';
	var search_opts = {};
	search_opts.name = 'manta';

	this.client.listApplications(search_opts, function (err, apps) {
		if (err)
			return (cb(err));

		if (apps.length === 0) {
			console.log('No manta application configured');
			return (cb(null));
		}
		var metadata = apps[0].metadata;
		var indexShards = metadata[common.INDEX_SHARDS];

		console.log(sprintf(fmt,
			'TYPE', 'SHARD NAME', 'READ ONLY STATUS'));

		if (indexShards) {
			for (var i = 0; i < indexShards.length; i++) {
				var roStatus = indexShards[i].readOnly ?
					indexShards[i].readOnly.toString() :
					'false';
				console.log(sprintf(fmt,
					'Index',
					indexShards[i].host,
					roStatus));
			}
		}

		if (metadata[common.MARLIN_SHARD])
			console.log(sprintf(fmt,
				'Marlin',
				metadata[common.MARLIN_SHARD],
				'--'));

		if (metadata[common.STORAGE_SHARD])
			console.log(sprintf(fmt,
				'Storage',
				metadata[common.STORAGE_SHARD],
				'--'));

		return (cb(null));
	});
};
Shardadm.prototype.do_list.help = (
	'List Manta shards.\n'
	+ '\n'
	+ 'Usage:\n'
	+ '     manta-shardadm list \n'
);

Shardadm.prototype.do_set = function (subcmd, opts, args, cb) {
	var self = this;

	if (args.length !== 0 || (!opts.i && !opts.m && !opts.s && !opts.r &&
		!opts.w)) {
		this.do_help('help', {}, [subcmd], cb);
		return;
	}

	var search_opts = {};
	search_opts.name = 'manta';

	this.client.listApplications(search_opts, function (err, apps) {
		if (err)
			return (cb(err));

		if (apps.length === 0) {
			console.log('no manta application configured');
			return (cb(null));
		}

		var app = apps[0];
		var domain_name = '.' + app.metadata['DOMAIN_NAME'];

		var metadata = {};
		var shards;
		var optsShards;
		var key;

		if (opts.m) {
			metadata[common.MARLIN_SHARD] =
				addSuffix(opts.m, domain_name);
		}

		if (opts.s) {
			metadata[common.STORAGE_SHARD] =
				addSuffix(opts.s, domain_name);
		}

		if (opts.i) {
			var indexShards = indexShardHash(opts.i, domain_name);
			metadata[common.INDEX_SHARDS] = indexShards;
		}

		if (opts.r) {
			/*
			 * Allow a user to pass comma-separated,
			 * space-separated, or comma-and-space-separated
			 * input.
			 */
			optsShards = opts.r.split(/[, ]+/);
			shards = app.metadata[common.INDEX_SHARDS].slice();
			optsShards.map(function (oshard) {
				for (key in shards) {
					if (oshard === shards[key].host)
						shards[key].readOnly = true;
				}
			});
			metadata[common.INDEX_SHARDS] = shards;
		}

		if (opts.w) {
			/*
			 * Allow a user to pass comma-separated,
			 * space-separated, or comma-and-space-separated
			 * input.
			 */
			optsShards = opts.w.split(/[, ]+/);
			shards = app.metadata[common.INDEX_SHARDS].slice();
			optsShards.map(function (oshard) {
				for (key in shards) {
					if ((oshard === shards[key].host) &&
						shards[key].readOnly === true)
					{
						delete shards[key].readOnly;
					}
				}
			});
			metadata[common.INDEX_SHARDS] = shards;
		}

		if (Object.keys(metadata).length === 0) {
			console.log('No shards to update');
			return (cb(null));
		}

		self.client.updateApplication(app.uuid,
			{ metadata: metadata },
				function (suberr) {
				if (suberr)
					return (cb(suberr));

				console.log(
					'Updated Manta shards successfully');
				return (cb(null));
				});
	});
};
Shardadm.prototype.do_set.options = [
	{
		names: [ 'i' ],
		type: 'string',
		help: 'shards for indexing tier'
	}, {
		names: [ 'm' ],
		type: 'string',
		help: 'shard for marlin job records'
	}, {
		names: [ 's' ],
		type: 'string',
		help: 'shard for minnow (manta_storage) records'
	}, {
		names: [ 'r' ],
		type: 'string',
		help: 'shard(s) to place in read-only mode'
	}, {
		names: [ 'w' ],
		type: 'string',
		help: 'shard(s) to place in writable mode'
	}
];
Shardadm.prototype.do_set.help = (
	'Set Manta shards.\n'
	+ '\n'
	+ 'Usage:\n'
	+ '     manta-shardadm set [OPTIONS] \n'
	+ '\n'
	+ '{{options}}'
);

/*
 * If the specified string doesn't already contain the specified suffix, add it.
 */
function addSuffix(str, suffix) {
	return (str.indexOf(suffix, str.length - suffix.length) === -1 ?
		str + suffix : str);
}

/*
 * Creates a hash of shards as host keys, and a key to designate the last shard
 * in the list for index shards.
 */
function indexShardHash(shardList, domainName) {
	assert.arrayOfString(shardList, 'shardList');

	var names = shardList.split(' ');
	var shards = [];

	names.forEach(function (name) {
		var shard = addSuffix(name, domainName);
		shards.push({ host: shard });
	});
	shards[shards.length - 1].last = true;
	return (shards);
}

var cli = new Shardadm();
cmdln.main(cli);
