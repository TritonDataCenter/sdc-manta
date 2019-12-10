/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
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

var log = new bunyan({
    name: 'tst.adm.js',
    level: process.env['LOG_LEVEL'] || 'warn',
    serializers: bunyan.stdSerializers
});

/*
 * This is our fake set of deployed services.  We'll construct a more concrete
 * version of this below, where we fill in SAPI instances, VMs objects, and CN
 * objects.
 */
var fakeDeployed = {
    cn001: {
        moray: {
            '1': {img002: 3},
            '2': {img002: 3},
            '3': {img002: 3}
        },
        webapi: {img004: 2}
    },
    cn002: {
        moray: {
            '1': {img002: 3},
            '2': {img002: 3},
            '3': {img002: 3}
        }
    },
    cn003: {
        postgres: {
            '1': {img003: 3},
            '2': {img003: 3},
            '3': {img003: 3}
        }
    },
    cn004: {
        postgres: {
            '1': {img003: 1},
            '2': {img003: 1}
        }
    }
};

function runTestCase(t, callback) {
    var adm, collector, desired;

    console.log('test case "%s"', t['name']);
    adm = new madm.MantaAdm(log);
    adm.loadFakeDeployed(common.generateFakeBase(fakeDeployed, 1));
    desired = jsprim.deepCopy(fakeDeployed);

    /* Sanity-check that we created the fake config properly */
    collector = new CollectorStream({});
    adm.dumpDeployedConfigByServiceJson(collector);
    assert.deepEqual(JSON.parse(collector.data), desired);

    t['changefunc'](desired);
    adm.readConfigRaw(JSON.stringify(desired));
    adm.generatePlan({}, function() {
        var actual = adm.dumpPlan();
        var expected = t['expect'];

        /*
         * We only care that properties specified in "expected" are also
         * present in "actual", not the other way around.
         */
        expected.forEach(function(ex, i) {
            for (var prop in actual[i]) {
                if (!ex.hasOwnProperty(prop)) {
                    delete actual[i][prop];
                }
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

vasync.forEachPipeline(
    {
        func: runTestCase,
        inputs: [
            {
                name: 'no change',
                changefunc: function(_config) {},
                expect: []
            },
            {
                name: 'remove one instance',
                changefunc: function(config) {
                    config['cn001']['webapi']['img004'] = 1;
                },
                expect: [
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'deprovision',
                        image: 'img004'
                    }
                ]
            },
            {
                name: 'deploy two instances',
                changefunc: function(config) {
                    config['cn001']['webapi']['img004'] = 4;
                },
                expect: [
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'provision',
                        image: 'img004'
                    },
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'provision',
                        image: 'img004'
                    }
                ]
            },
            {
                name: 'remove a service',
                changefunc: function(config) {
                    delete config['cn001']['webapi'];
                },
                expect: [
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'deprovision',
                        image: 'img004'
                    },
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'deprovision',
                        image: 'img004'
                    }
                ]
            },
            {
                name: 'remove a CN',
                changefunc: function(config) {
                    delete config['cn004'];
                },
                expect: [
                    {
                        cnid: 'cn004',
                        service: 'postgres',
                        action: 'deprovision',
                        image: 'img003',
                        shard: '1'
                    },
                    {
                        cnid: 'cn004',
                        service: 'postgres',
                        action: 'deprovision',
                        image: 'img003',
                        shard: '2'
                    }
                ]
            },
            {
                name: 'add a service',
                changefunc: function(config) {
                    config['cn004']['webapi'] = {img004: 2};
                },
                expect: [
                    {
                        cnid: 'cn004',
                        service: 'webapi',
                        action: 'provision',
                        image: 'img004'
                    },
                    {
                        cnid: 'cn004',
                        service: 'webapi',
                        action: 'provision',
                        image: 'img004'
                    }
                ]
            },
            {
                name: 'add a CN',
                changefunc: function(config) {
                    config['cn005'] = {webapi: {img004: 1}};
                },
                expect: [
                    {
                        cnid: 'cn005',
                        service: 'webapi',
                        action: 'provision',
                        image: 'img004'
                    }
                ]
            },
            {
                name: 'upgrade a service',
                changefunc: function(config) {
                    config['cn001']['webapi']['img004'] = 1;
                    config['cn001']['webapi']['img005'] = 2;
                },
                expect: [
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'reprovision',
                        image: 'img005'
                    },
                    {
                        cnid: 'cn001',
                        service: 'webapi',
                        action: 'provision',
                        image: 'img005'
                    }
                ]
            },
            {
                name: 'upgrade a service (with shard)',
                changefunc: function(config) {
                    config['cn001']['moray']['2']['img003'] = 1;
                    config['cn001']['moray']['2']['img002'] = 1;
                },
                expect: [
                    {
                        cnid: 'cn001',
                        service: 'moray',
                        shard: '2',
                        action: 'reprovision',
                        image: 'img003'
                    },
                    {
                        cnid: 'cn001',
                        service: 'moray',
                        shard: '2',
                        action: 'deprovision',
                        image: 'img002'
                    }
                ]
            },
            {
                name: 'provision/deprovision in different shards (not upgrade)',
                changefunc: function(config) {
                    config['cn001']['moray']['2']['img003'] = 1;
                    config['cn001']['moray']['1']['img002'] = 2;
                },
                expect: [
                    {
                        cnid: 'cn001',
                        service: 'moray',
                        shard: '1',
                        action: 'deprovision',
                        image: 'img002'
                    },
                    {
                        cnid: 'cn001',
                        service: 'moray',
                        shard: '2',
                        action: 'provision',
                        image: 'img003'
                    }
                ]
            }
        ]
    },
    function(err) {
        if (err) {
            throw err;
        }
        console.log('TEST PASSED');
    }
);
