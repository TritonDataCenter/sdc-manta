/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.adm.js: tests manta-adm planner functionality
 */

var assert = require('assert');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var vasync = require('vasync');
var CollectorStream = require('./CollectorStream');
var VError = require('verror').VError;

var common = require('./common');
var madm = require('../lib/adm');

function runTestCase(t, callback)
{
	var version, adm, collector, desired;

	console.log('test case "%s"', t['name']);
	version = t.version || 2;
	adm = new madm.MantaAdm(log);
	adm.loadFakeDeployed(common.generateFakeBase(
	    fakeDeployed[version], 1), t.version);
	desired = jsprim.deepCopy(fakeDeployed[version]);

	/* Sanity-check that we created the fake config properly */
	collector = new CollectorStream({});
	adm.dumpDeployedConfigByServiceJson(collector);
	assert.deepEqual(JSON.parse(collector.data), desired);

	t['changefunc'](desired);
	adm.readConfigRaw(JSON.stringify(desired));
	adm.generatePlan(function () {
		var actual = adm.dumpPlan();
		var expected = t['expect'];

		/*
		 * We only care that properties specified in "expected" are also
		 * present in "actual", not the other way around.
		 */
		expected.forEach(function (ex, i) {
			for (var prop in actual[i]) {
				if (!ex.hasOwnProperty(prop))
					delete (actual[i][prop]);
			}
		});

		if (jsprim.deepEqual(actual, expected)) {
			console.log('test case "%s" passed', t['name']);
			callback();
			return;
		}
		console.log('test case "%s" FAILED', t['name']);
		console.error('EXPECTED:\n', expected);
		console.error('FOUND:\n', actual);
		callback(new VError('TEST FAILED: "%s"', t['name']));
	});
}

/*
 * Mainline
 */

var log = new bunyan({
    'name': 'tst.adm.js',
    'level': process.env['LOG_LEVEL'] || 'warn',
    'serializers': bunyan.stdSerializers
});

/*
 * This is our fake set of deployed services.  We'll construct a more concrete
 * version of this below, where we fill in SAPI instances, VMs objects, and CN
 * objects.
 */
var fakeDeployed = {
    1: {
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
    },
    2: {
	'metadata': { 'v': 2 },
	'cn001': {
	    'marlin': [ { 'image_uuid': 'img001', 'count': 10 } ],
	    'moray': [
		{ 'shard': '1', 'image_uuid': 'img002', 'count': 3 },
		{ 'shard': '2', 'image_uuid': 'img002', 'count': 3 },
		{ 'shard': '3', 'image_uuid': 'img002', 'count': 3 }
	    ],
	    'medusa': [ { 'image_uuid': 'img004', 'count': 2 } ]
	},
	'cn002': {
	    'marlin': [ { 'image_uuid': 'img001', 'count': 10 } ],
	    'moray': [
		{ 'shard': '1', 'image_uuid': 'img002', 'count': 3 },
		{ 'shard': '2', 'image_uuid': 'img002', 'count': 3 },
		{ 'shard': '3', 'image_uuid': 'img002', 'count': 3 }
	    ]
	},
	'cn003': {
	    'marlin': [ { 'image_uuid': 'img001', 'count': 10 } ],
	    'postgres': [
		{ 'shard': '1', 'image_uuid': 'img003', 'count': 3 },
		{ 'shard': '2', 'image_uuid': 'img003', 'count': 3 },
		{ 'shard': '3', 'image_uuid': 'img003', 'count': 3 }
	    ]
	},
	'cn004': {
	    'loadbalancer': [ {
		'image_uuid': 'img004',
		'count': 2
	    }, {
		'image_uuid': 'img004',
		'count': 1,
		'untrusted_networks': [ {
		    'ipv4_uuid': 'external',
		    'primary': true
		}, {
		    'ipv4_uuid': 'net002'
		} ]
	    } ],
	    'marlin': [ { 'image_uuid': 'img001', 'count': 2 } ],
	    'postgres': [
		{ 'shard': '1', 'image_uuid': 'img003', 'count': 1 },
		{ 'shard': '2', 'image_uuid': 'img003', 'count': 1 }
	    ]
	}
    }
};

vasync.forEachPipeline({
    'func': runTestCase,
    'inputs': [ {
	'name': 'no change',
	'changefunc': function (config) {},
	'expect': [],
	'version': 1
    }, {
	'name': 'remove one instance',
	'changefunc': function (config) {
		config['cn001']['medusa']['img004'] = 1;
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'deprovision',
	    'image': 'img004'
	} ],
	'version': 1
    }, {
	'name': 'deploy two instances',
	'changefunc': function (config) {
		config['cn001']['medusa']['img004'] = 4;
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'provision',
	    'image': 'img004'
	}, {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'provision',
	    'image': 'img004'
	} ],
	'version': 1
    }, {
	'name': 'remove a service',
	'changefunc': function (config) {
		delete (config['cn001']['medusa']);
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'deprovision',
	    'image': 'img004'
	}, {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'deprovision',
	    'image': 'img004'
	} ],
	'version': 1
    }, {
	'name': 'remove a CN',
	'changefunc': function (config) {
		delete (config['cn004']);
	},
	'expect': [ {
	    'cnid': 'cn004',
	    'service': 'postgres',
	    'action': 'deprovision',
	    'image': 'img003',
	    'shard': '1'
	}, {
	    'cnid': 'cn004',
	    'service': 'postgres',
	    'action': 'deprovision',
	    'image': 'img003',
	    'shard': '2'
	}, {
	    'cnid': 'cn004',
	    'service': 'marlin',
	    'action': 'deprovision',
	    'image': 'img001'
	}, {
	    'cnid': 'cn004',
	    'service': 'marlin',
	    'action': 'deprovision',
	    'image': 'img001'
	} ],
	'version': 1
    }, {
	'name': 'add a service',
	'changefunc': function (config) {
		config['cn004']['medusa'] = { 'img004': 2 };
	},
	'expect': [ {
	    'cnid': 'cn004',
	    'service': 'medusa',
	    'action': 'provision',
	    'image': 'img004'
	}, {
	    'cnid': 'cn004',
	    'service': 'medusa',
	    'action': 'provision',
	    'image': 'img004'
	} ],
	'version': 1
    }, {
	'name': 'add a CN',
	'changefunc': function (config) {
		config['cn005'] = { 'medusa': { 'img004': 1 } };
	},
	'expect': [ {
	    'cnid': 'cn005',
	    'service': 'medusa',
	    'action': 'provision',
	    'image': 'img004'
	} ],
	'version': 1
    }, {
	'name': 'upgrade a service (non-marlin)',
	'changefunc': function (config) {
		config['cn001']['medusa']['img004'] = 1;
		config['cn001']['medusa']['img005'] = 2;
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'reprovision',
	    'image': 'img005'
	}, {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'provision',
	    'image': 'img005'
	} ],
	'version': 1
    }, {
	'name': 'upgrade a service (marlin)',
	'changefunc': function (config) {
		config['cn001']['marlin']['img001'] = 8;
		config['cn001']['marlin']['img002'] = 2;
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'marlin',
	    'action': 'provision',
	    'image': 'img002'
	}, {
	    'cnid': 'cn001',
	    'service': 'marlin',
	    'action': 'deprovision',
	    'image': 'img001'
	}, {
	    'cnid': 'cn001',
	    'service': 'marlin',
	    'action': 'provision',
	    'image': 'img002'
	}, {
	    'cnid': 'cn001',
	    'service': 'marlin',
	    'action': 'deprovision',
	    'image': 'img001'

	} ],
	'version': 1
    }, {
	'name': 'upgrade a service (with shard)',
	'changefunc': function (config) {
		config['cn001']['moray']['2']['img003'] = 1;
		config['cn001']['moray']['2']['img002'] = 1;
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'moray',
	    'shard': '2',
	    'action': 'reprovision',
	    'image': 'img003'
	}, {
	    'cnid': 'cn001',
	    'service': 'moray',
	    'shard': '2',
	    'action': 'deprovision',
	    'image': 'img002'
	} ],
	'version': 1
    }, {
	'name': 'provision/deprovision in different shards (not upgrade)',
	'changefunc': function (config) {
		config['cn001']['moray']['2']['img003'] = 1;
		config['cn001']['moray']['1']['img002'] = 2;
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'moray',
	    'shard': '1',
	    'action': 'deprovision',
	    'image': 'img002'
	}, {
	    'cnid': 'cn001',
	    'service': 'moray',
	    'shard': '2',
	    'action': 'provision',
	    'image': 'img003'
	} ],
	'version': 1
    }, {
	'name': 'v2: provision with specific networks',
	'changefunc': function (config) {
	    config['cn001']['marlin'].push({
		'image_uuid': 'img001',
		'untrusted_networks': [ {
		    'ipv4_uuid': 'net002',
		    'primary': true
		}, {
		    'ipv4_uuid': 'net003'
		} ],
		'count': 1
	    });
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'marlin',
	    'action': 'provision',
	    'image': 'img001',
	    'networks': [ {
		'ipv4_uuid': 'net002',
		'primary': true
	    }, {
		'ipv4_uuid': 'net003'
	    } ]
	} ]
    }, {
	'name': 'v2: provision with default networks',
	'changefunc': function (config) {
	    config['cn002']['marlin'][0]['count']++;
	},
	'expect': [ {
	    'cnid': 'cn002',
	    'service': 'marlin',
	    'action': 'provision',
	    'image': 'img001',
	    'networks': '-'
	} ]
    }, {
	'name': 'v2: provision with no networks (where default >=1)',
	'changefunc': function (config) {
	    config['cn002']['marlin'].push({
		'image_uuid': 'img001',
		'count': 1,
		'untrusted_networks': []
	    });
	},
	'expect': [ {
	    'cnid': 'cn002',
	    'service': 'marlin',
	    'action': 'provision',
	    'image': 'img001',
	    'networks': []
	} ]
    }, {
	'name': 'v2: upgrade a service (non-marlin)',
	'changefunc': function (config) {
	    config['cn001']['medusa'][0]['image_uuid'] = 'img999';
	},
	'expect': [ {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'reprovision',
	    'image': 'img999'
	}, {
	    'cnid': 'cn001',
	    'service': 'medusa',
	    'action': 'reprovision',
	    'image': 'img999'
	} ]
    }, {
	'name': 'v2: re-order networks',
	'changefunc': function (config) {
	    config['cn004']['loadbalancer'][1]['untrusted_networks'] = [ {
		'ipv4_uuid': 'net002'
	    }, {
		'ipv4_uuid': 'external',
		'primary': true
	    } ];
	},
	'expect': []
    } ]
}, function (err) {
	if (err)
		throw (err);
	console.log('TEST PASSED');
});
