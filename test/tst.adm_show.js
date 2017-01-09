/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.adm_show.js: tests manta-adm show functionality
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var vasync = require('vasync');

var common = require('./common');
var madm = require('../lib/adm');

var nrun = 0;
var separator = '--------------------------------------------------';

function main() {
	vasync.forEachPipeline({
		'func': runTestCase,
		'inputs': testCases
	}, function (err) {
		assertplus.ok(!err);
		assertplus.equal(nrun, testCases.length);
		console.error('%d test cases run', nrun);
	});
}

function runTestCase(t, callback) {
	assertplus.string(t.name);
	assertplus.ok(typeof (t.config) == 'object');
	assertplus.ok(typeof (t.func) == 'function');

	console.log(separator);
	console.log('test case "%s"', t.name);

	t.func.call(adm, process.stdout, t.config);

	console.log(separator);
	nrun++;
	callback();
}

var fakeDeployed = {
    'cn001': {
	'marlin': { 'img001': 10 },
	'moray': {
	    '1': { 'img002': 3 },
	    '2': { 'img002': 3 },
	    '3': { 'img002': 3 }
	},
	'medusa': { 'img004': 2 }
    },
    'cn002': {
	'marlin': { 'img001': 10 },
	'moray': {
	    '1': { 'img002': 3 },
	    '2': { 'img002': 3 },
	    '3': { 'img002': 3 }
	}
    },
    'cn003': {
	'marlin': { 'img001': 10 },
	'postgres': {
	    '1': { 'img003': 3 },
	    '2': { 'img003': 3 },
	    '3': { 'img003': 3 }
	}
    },
    'cn004': {
	'marlin': { 'img001': 2 },
	'postgres': {
	    '1': { 'img003': 1 },
	    '2': { 'img003': 1 }
	}
    }
};

var log = new bunyan({
    'name': 'tst.adm_show.js',
    'level': process.env['LOG_LEVEL'] || 'warn',
    'serializers': bunyan.stdSerializers
});

var adm = new madm.MantaAdm(log);
adm.loadFakeDeployed(common.generateFakeBase(fakeDeployed, 3));

var testCases = [ {
	'name': 'manta-adm show',
	'func': adm.dumpDeployedZonesByService,
	'config': {}
}, {
	'name': 'manta-adm show -c',
	'func': adm.dumpDeployedZonesByCn,
	'config': {}
}, {
	'name': 'manta-adm show -s',
	'func': adm.dumpDeployedConfigByService,
	'config': {}
}, {
	'name': 'manta-adm show -a',
	'func': adm.dumpDeployedZonesByService,
	'config': {
	    'doall': true
	}
}, {
	'name': 'manta-adm show -H',
	'func': adm.dumpDeployedZonesByService,
	'config': {
	    'omitHeader': true
	}
}, {
	'name': 'manta-adm show -a -o service,datacenter,shard,version',
	'func': adm.dumpDeployedZonesByService,
	'config': {
	    'doall': true,
	    'columns': ['service', 'datacenter', 'shard', 'version']
	}
}, {
	'name': 'manta-adm show -js',
	'func': adm.dumpDeployedConfigByServiceJson,
	'config': {}
}];

main();
