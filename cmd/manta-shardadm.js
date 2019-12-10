#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
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
var sprintf = require('extsprintf').sprintf;
var util = require('util');

var Cmdln = cmdln.Cmdln;
var Logger = require('bunyan');

var VERSION = '1.0.0';

function Shardadm() {
    Cmdln.call(this, {
        name: 'manta-shardadm',
        desc: 'Manage manta shards',
        // Custom options. By default you get -h/--help.
        options: [
            {names: ['help', 'h'], type: 'bool', help: 'Print help and exit.'},
            {name: 'version', type: 'bool', help: 'Print version and exit.'}
        ]
    });
}
util.inherits(Shardadm, Cmdln);

Shardadm.prototype.init = function(opts, args, cb) {
    if (opts.version) {
        console.log(this.name, VERSION);
        cb(false);
        return;
    }

    this.log = new Logger({
        name: __filename,
        serializers: Logger.stdSerializers,
        streams: [
            {
                level: 'debug',
                path: '/var/log/manta-shardadm.log'
            }
        ]
    });

    var CFG = path.resolve(__dirname, '../etc/config.json');
    var config = JSON.parse(fs.readFileSync(CFG, 'utf8'));

    this.client = new sdc.SAPI({
        url: config.sapi.url,
        log: this.log,
        agent: false,
        version: '~2'
    });

    Cmdln.prototype.init.apply(this, arguments);
};

Shardadm.prototype.do_list = function(subcmd, opts, args, cb) {
    var search_opts = {};
    search_opts.name = 'manta';

    this.client.listApplications(search_opts, function(err, apps) {
        if (err) {
            return cb(err);
        }

        if (apps.length === 0) {
            console.log('No manta application configured');
            return cb(null);
        }

        printShards(apps[0].metadata, cb);
    });
};
Shardadm.prototype.do_list.help = 'List shards';

Shardadm.prototype.do_list.help = [
    'List Manta shards.',
    '',
    'Usage:',
    '     manta-shardadm list'
].join('\n');

function printShards(metadata, cb) {
    var i;
    var fmt = '%-12s %s';

    console.log(sprintf(fmt, 'TYPE', 'SHARD NAME'));

    if (metadata[common.INDEX_SHARDS]) {
        for (i = 0; i < metadata[common.INDEX_SHARDS].length; i++) {
            console.log(
                sprintf(fmt, 'Index', metadata[common.INDEX_SHARDS][i].host)
            );
        }
    }

    if (metadata[common.MARLIN_SHARD]) {
        console.log(sprintf(fmt, 'Marlin', metadata[common.MARLIN_SHARD]));
    }
    if (metadata[common.STORAGE_SHARD]) {
        console.log(sprintf(fmt, 'Storage', metadata[common.STORAGE_SHARD]));
    }

    if (metadata[common.BUCKETS_SHARDS]) {
        for (i = 0; i < metadata[common.BUCKETS_SHARDS].length; i++) {
            console.log(
                sprintf(fmt, 'Buckets', metadata[common.BUCKETS_SHARDS][i].host)
            );
        }
    }

    return cb(null);
}

Shardadm.prototype.do_set = function(subcmd, opts, args, cb) {
    var self = this;

    if (args.length !== 0 || (!opts.i && !opts.b && !opts.m && !opts.s)) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    }

    var search_opts = {};
    search_opts.name = 'manta';

    this.client.listApplications(search_opts, function(err, apps) {
        if (err) {
            return cb(err);
        }

        if (apps.length === 0) {
            console.log('no manta application configured');
            return cb(null);
        }

        var app = apps[0];
        var domain_name = '.' + app.metadata['DOMAIN_NAME'];

        var metadata = {};

        if (opts.m) {
            metadata[common.MARLIN_SHARD] = addSuffix(opts.m, domain_name);
        }

        if (opts.s) {
            metadata[common.STORAGE_SHARD] = addSuffix(opts.s, domain_name);
        }

        if (opts.i) {
            addIndexShards(opts.i, common.INDEX_SHARDS);
        }

        if (opts.b) {
            addIndexShards(opts.b, common.BUCKETS_SHARDS);
        }

        if (Object.keys(metadata).length === 0) {
            console.log('No shards to update');
            return cb(null);
        }

        self.client.updateApplication(app.uuid, {metadata: metadata}, function(
            suberr
        ) {
            if (suberr) {
                return cb(suberr);
            }

            console.log('Updated Manta shards successfully');
            return cb(null);
        });

        /*
         * Helper function for adding index shards to metadata object.
         * Arguments are:
         *
         * - nameStr: a string of shard names separated by spaces
         *
         * - key: the field in the metadata object in which to store
         *   the shard array
         */
        function addIndexShards(nameStr, key) {
            var names = nameStr.split(' ');
            var shards = [];

            names.forEach(function(name) {
                var shard = addSuffix(name, domain_name);
                shards.push({host: shard});
            });
            shards[shards.length - 1].last = true;

            metadata[key] = shards;
        }
    });
};
Shardadm.prototype.do_set.options = [
    {
        names: ['i'],
        type: 'string',
        help: 'shards for indexing tier'
    },
    {
        names: ['b'],
        type: 'string',
        help: 'shards for manta buckets subsystem indexing tier'
    },
    {
        names: ['m'],
        type: 'string',
        help: 'shard for marlin job records'
    },
    {
        names: ['s'],
        type: 'string',
        help: 'shard for minnow (manta_storage) records'
    }
];
Shardadm.prototype.do_set.help =
    'Set Manta shards.\n' +
    '\n' +
    'Usage:\n' +
    '     manta-shardadm set [OPTIONS] \n' +
    '\n' +
    '{{options}}';

/*
 * If the specified string doesn't already contain the specified suffix, add it.
 */
function addSuffix(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) === -1
        ? str + suffix
        : str;
}

var cli = new Shardadm();
cmdln.main(cli);
