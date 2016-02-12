/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.layout.js: tests manta-adm automatic layout functionality
 *
 * This test works by providing a bunch of configuration objects to the
 * auto-layout mechanism and generating textual output the same way that
 * "manta-adm genconfig --from-file=..." would.  The output is automatically
 * verified against the expected output by the test runner.  This works because
 * the layout generator is deterministic, and this approach is much simpler than
 * hand-checking all the conditions we would like to verify about each generated
 * configuration.  Of course, as the implementation changes, new expected output
 * has to be generated and verified by hand.
 */

var assertplus = require('assert-plus');
var fs = require('fs');
var jsprim = require('jsprim');
var sprintf = require('extsprintf').sprintf;
var vasync = require('vasync');
var VError = require('verror');

var layout = require('../lib/layout');

/*
 * Each test case has a "name" and either:
 *
 *     json     text string that will be parsed as JSON
 *
 *     config   JavaScript object that would be the result of JSON.parse()
 *
 * The "json" test cases exercise more code paths, but the "config" ones are
 * easier to manage here, particularly for more complex cases.
 */
var testcases = [ {
    'name': 'invalid config: invalid JSON (unexpected EOF)',
    'json': ''
}, {
    'name': 'invalid config: invalid JSON (unexpected "}")',
    'json': '}'
}, {
    'name': 'invalid config: bad type',
    'json': 'true'
}, {
    'name': 'raw JSON: trivial case',
    'json': JSON.stringify({
	'nshards': 3,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('metadata', 1, 0),
	    mkserver('metadata', 2, 0),
	    mkserver('storage',  0, 0),
	    mkserver('storage',  1, 0)
	]
    }, null, '    ')
}, {
    'name': 'invalid config: missing value (nshards)',
    'config': {
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('storage',  0, 0)
	]
    }
}, {
    'name': 'invalid config: bad type (nshards)',
    'config': {
	'nshards': 3.2,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('storage',  0, 0)
	]
    }
}, {
    'name': 'invalid config: bad value (nshards)',
    'config': {
	'nshards': -3,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('storage',  0, 0)
	]
    }
}, {
    'name': 'invalid config: missing value (servers)',
    'config': {
	'nshards': 3
    }
}, {
    'name': 'invalid config: bad type (servers)',
    'config': {
	'nshards': 3,
	'servers': 7
    }
}, {
    'name': 'invalid config: empty list of servers',
    'config': {
	'nshards': 3,
	'servers': []
    }
}, {
    'name': 'invalid config: bad type for server',
    'config': {
	'nshards': 3,
	'servers': [ 'foobar' ]
    }
}, {
    'name': 'invalid config: server property bad value: "type"',
    'config': {
	'nshards': 3,
	'servers': [ { 'type': 'junk' } ]
    }
}, {
    'name': 'invalid config: server property missing ("type")',
    'config': {
	'nshards': 3,
	'servers': [ {} ]
    }
}, {
    'name': 'invalid config: server property missing ("uuid")',
    'config': {
	'nshards': 3,
	'servers': [ { 'type': 'metadata' } ]
    }
}, {
    'name': 'invalid config: server property missing ("memory")',
    'config': {
	'nshards': 3,
	'servers': [ { 'type': 'metadata', 'uuid': 'junkuuid' } ]
    }
}, {
    'name': 'invalid config: bad type for server property ("uuid")',
    'config': {
	'nshards': 3,
	'servers': [ { 'type': 'metadata', 'uuid': 17, 'memory': 3 } ]
    }
}, {
    'name': 'invalid config: bad type for server property ("memory")',
    'config': {
	'nshards': 3,
	'servers': [ { 'type': 'metadata', 'uuid': 'junkuuid', 'memory': {} } ]
    }
}, {
    'name': 'invalid config: no storage servers',
    'config': {
	'nshards': 3,
	'servers': [ mkserver('metadata', 0, 0) ]
    }
}, {
    'name': 'invalid config: no metadata servers',
    'config': {
	'nshards': 3,
	'servers': [ mkserver('storage', 0, 0) ]
    }
}, {
    'name': 'invalid config: duplicate server',
    'config': {
	'nshards': 3,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('storage',  0, 0),
	    mkserver('metadata', 0, 0)
	]
    }
}, {
    'name': 'invalid config: same rack in different AZs',
    'config': {
	'nshards': 3,
	'servers': [ {
	    'type': 'metadata',
	    'uuid': 's000',
	    'rack': 'rack0',
	    'az': 'az1',
	    'memory': 128
	}, {
	    'type': 'storage',
	    'uuid': 's001',
	    'rack': 'rack0',
	    'az': 'az2',
	    'memory': 128
	} ]
    }
}, {
    'name': 'unsupported config: multiple AZs',
    'config': {
	'nshards': 3,
	'servers': [ {
	    'type': 'metadata',
	    'uuid': 's000',
	    'rack': 'rack0',
	    'az': 'az1',
	    'memory': 128
	}, {
	    'type': 'storage',
	    'uuid': 's001',
	    'rack': 'rack1',
	    'az': 'az2',
	    'memory': 128
	} ]
    }
}, {
    'name': 'trivial two-system case, 1 shard',
    'config': {
	'nshards': 1,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('storage',  0, 0)
	]
    }
}, {
    'name': 'trivial two-system case, 3 shards',
    'config': {
	'nshards': 3,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('storage',  0, 0)
	]
    }
}, {
    'name': '3-rack, 4-shard deployment',
    'config': {
	'nshards': 4,
	'servers': [
	    mkserver('metadata', 0, 0),
	    mkserver('metadata', 0, 1),
	    mkserver('metadata', 0, 2),
	    mkserver('metadata', 0, 3),
	    mkserver('storage',  0, 0),
	    mkserver('storage',  0, 1),

	    mkserver('metadata', 1, 0),
	    mkserver('metadata', 1, 1),
	    mkserver('metadata', 1, 2),
	    mkserver('metadata', 1, 3),
	    mkserver('storage',  1, 0),
	    mkserver('storage',  1, 1),

	    mkserver('metadata', 2, 0),
	    mkserver('metadata', 2, 1),
	    mkserver('metadata', 2, 2),
	    mkserver('metadata', 2, 3),
	    mkserver('storage',  2, 0),
	    mkserver('storage',  2, 1)
	]
    }
} ];

/*
 * We use dummy image names to keep output deterministic and avoid any
 * dependency on fetching real current images.
 */
var images = {
    'nameservice': 'NAMESERVICE_IMAGE0',
    'postgres': 'POSTGRES_IMAGE0',
    'moray': 'MORAY_IMAGE0',
    'electric-moray': 'ELECTRIC_MORAY_IMAGE0',
    'storage': 'STORAGE_IMAGE0',
    'authcache': 'AUTHCACHE_IMAGE0',
    'webapi': 'WEBAPI_IMAGE0',
    'loadbalancer': 'LOADBALANCER_IMAGE0',
    'jobsupervisor': 'JOBSUPERVISOR_IMAGE0',
    'jobpuller': 'JOBPULLER_IMAGE0',
    'medusa': 'MEDUSA_IMAGE0',
    'ops': 'OPS_IMAGE0',
    'madtom': 'MADTOM_IMAGE0',
    'marlin-dashboard': 'DASHBOARD_IMAGE0',
    'marlin': 'MARLIN_IMAGE0'
};

var nrun = 0;
var separator = '--------------------------------------------------';

function main()
{
	vasync.forEachPipeline({
	    'inputs': testcases,
	    'func': runTestCase
	}, function (err) {
		assertplus.ok(!err);
		assertplus.equal(nrun, testcases.length);
		console.error('%d test cases run', nrun);
	});
}

/*
 * Generate an object representing a server of type "role" in rack "racknum".
 * This will be server "role" + "servernum" within this rack.
 */
function mkserver(role, racknum, servernum)
{
	var rack, cnid;

	assertplus.ok([ 'metadata', 'storage' ].indexOf(role) != -1);
	assertplus.number(racknum);
	assertplus.ok(racknum >= 0);
	assertplus.ok(racknum < 100);
	assertplus.number(servernum);
	assertplus.ok(servernum >= 0);
	assertplus.ok(servernum < 100);

	rack = sprintf('rack_r%02d', racknum);
	cnid = sprintf('server_r%02d_%s%02d', racknum, role, servernum);

	return ({
	    'type': role,
	    'uuid': cnid,
	    'memory': 64,
	    'rack': rack
	});
}

/*
 * Executes one of the testcases defined above.  This involves preparing the
 * input, loading it, generating a layout, and dumping the layout, warnings, and
 * errors to stdout.
 */
function runTestCase(t, callback)
{
	var stages, tcstate;

	assertplus.string(t.name);
	assertplus.ok(typeof (t.json) == 'string' ||
	    typeof (t.config) == 'object');

	stages = [];
	tcstate = {};
	tcstate.tc_loader = new layout.DcConfigLoader();
	tcstate.tc_testcase = t;

	if (t.hasOwnProperty('json')) {
		tcstate.tc_tmpfile = '/tmp/tst.layout.js';
		stages.push(runTestCaseWriteConfig);
		stages.push(runTestCaseLoadFromFile);
		stages.push(runTestCaseRemoveConfig);
	} else {
		stages.push(runTestCaseLoadDirectly);
	}

	stages.push(runTestCaseGenerate);

	console.log(separator);
	console.log('test case: %s', t.name);
	vasync.pipeline({
	    'funcs': stages,
	    'arg': tcstate
	}, callback);
}

/*
 * For test cases that provide raw text to be parsed as JSON, those need to be
 * loaded from a file, so we write a temporary file here.
 */
function runTestCaseWriteConfig(tcstate, callback)
{
	var out, error;

	assertplus.string(tcstate.tc_tmpfile);
	assertplus.string(tcstate.tc_testcase.json);
	assertplus.ok(!tcstate.tc_testcase.hasOwnProperty('config'));

	console.log('input: %s', tcstate.tc_testcase.json);
	out = fs.createWriteStream(tcstate.tc_tmpfile);
	out.on('error', function (err) {
		error = true;
		callback(new VError(err, 'write "%s"', tcstate.tc_tmpfile));
	});

	out.end(tcstate.tc_testcase.json);
	out.on('close', function () {
		if (!error) {
			callback();
		}
	});
}

/*
 * For test cases that provide raw text, load the temporary file created
 * earlier.
 */
function runTestCaseLoadFromFile(tcstate, callback)
{
	assertplus.string(tcstate.tc_tmpfile);
	tcstate.tc_loader.loadFromFile({
	    'filename': tcstate.tc_tmpfile
	}, function (err, dcconfig) {
		if (err) {
			/*
			 * Recall that our stdout is compared against expected
			 * output.  Checking for expected error message is part
			 * of that.  We don't need to do anything here except
			 * make sure messages get printed and make sure we skip
			 * the rest of the pipeline in this case.
			 */
			console.log('ERROR: %s', err.message);
			tcstate.tc_dcconfig = null;
		} else {
			tcstate.tc_dcconfig = dcconfig;
		}

		callback();
	});
}

/*
 * For test cases that provide raw text, remove the temporary file.
 */
function runTestCaseRemoveConfig(tcstate, callback)
{
	assertplus.string(tcstate.tc_tmpfile);
	fs.unlink(tcstate.tc_tmpfile, callback);
}

/*
 * For test cases that provide an object directly, load it.
 */
function runTestCaseLoadDirectly(tcstate, callback)
{
	assertplus.ok(!tcstate.tc_testcase.hasOwnProperty('json'));
	assertplus.object(tcstate.tc_testcase.config);

	console.log('input: %s', JSON.stringify(
	    tcstate.tc_testcase.config, null, 4));
	tcstate.tc_loader.loadDirectly({
	    'config': tcstate.tc_testcase.config
	}, function (err, dcconfig) {
		if (err) {
			/*
			 * See note above in runTestCaseLoadFromFile().
			 */
			console.log('ERROR: %s', err.message);
			tcstate.tc_dcconfig = null;
		} else {
			tcstate.tc_dcconfig = dcconfig;
		}

		callback();
	});
}

/*
 * The meat of the test case: generate a layout and dump it to stdout.
 */
function runTestCaseGenerate(tcstate, callback)
{
	var svclayout;

	/* See notes about error handling above. */
	if (tcstate.tc_dcconfig !== null) {
		assertplus.object(tcstate.tc_dcconfig);

		svclayout = layout.generateLayout({
		    'dcconfig': tcstate.tc_dcconfig,
		    'images': images
		});

		console.log('\ngenerated config:');
		svclayout.serialize(process.stdout, process.stdout);
	}

	console.log(separator);
	nrun++;
	callback();
}

main();
