/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * tst.adm.js: tests "manta-adm zk list" functionality
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var vasync = require('vasync');
var CollectorStream = require('./CollectorStream');
var VError = require('verror').VError;
var readable = require('readable-stream');
var util = require('util');
var sprintf = require('extsprintf').sprintf;

var madm = require('../lib/adm');

var outputHeader =
    '# DATACENTER ZONENAME                             IP                PORT';
var localDc = 'testdc1';
var remoteDc = 'testdc2';
var verbose = process.argv[2] == '-v' ? true : false;

/*
 * Helper functions
 */
function outputRow(num, dc, vmuuid, ip)
{
	return (sprintf('%d %-10s %-36s %-16s %5d', num,
	    dc || '-', vmuuid || '-', ip, 2181));
}

function runTestCase(testcase, callback)
{
	var deployed, svcid, byip;
	var collector, adm, problems, warnings, expected;
	var i, j;

	assertplus.object(testcase, 'testcase');
	assertplus.string(testcase.name, 'testcase.name');
	assertplus.string(testcase.output, 'testcase.output');
	assertplus.arrayOfString(testcase.zk_servers, 'testcase.zk_servers');
	assertplus.arrayOfString(testcase.cns, 'testcase.cns');
	assertplus.arrayOfObject(testcase.instances, 'testcase.instances');
	assertplus.arrayOfObject(testcase.warnings,
	    'testcase.warnings');
	assertplus.func(callback, 'callback');

	console.error('test case: %s', testcase['name']);

	byip = {};
	deployed = {};
	deployed.app = {};
	deployed.app.name = 'manta';
	deployed.app.metadata = {};
	deployed.app.metadata.ZK_SERVERS =
	    testcase.zk_servers.map(function (ip, num0) {
		var zkid = num0 + 1;
		byip[ip] = zkid;
		return ({
		    'host': ip,
		    'port': 2181,
		    'num': zkid
		});
	    });
	deployed.app.metadata.ZK_SERVERS[
	    deployed.app.metadata.ZK_SERVERS.length - 1].last = true;

	svcid = 'svc001';
	deployed.services = {};
	deployed.services[svcid] = { 'name': 'nameservice' };
	deployed.instances = {};
	deployed.instances[svcid] = [];

	deployed.cns = {};
	testcase.cns.forEach(function (cnid) {
		deployed.cns[cnid] = {
		    'datacenter': localDc,
		    'hostname': cnid.toUpperCase(),
		    'server_uuid': cnid,
		    'sysinfo': {
			'Network Interfaces': {}
		    }
		};
	});

	deployed.vms = {};
	deployed.images = {};
	testcase.instances.forEach(function (instance) {
		var uuid, server_uuid, islocal;

		uuid = instance['vm'];
		server_uuid = instance['cn'];
		islocal = deployed.cns.hasOwnProperty(server_uuid) &&
		    deployed.cns[server_uuid] !== null;
		if (islocal) {
			if (instance.zone_removed !== true) {
				deployed.vms[uuid] = {
				    'nics': [ {
					'primary': true,
					'ip': instance['ip']
				    } ],
				    'server_uuid': server_uuid
				};
			}
		}

		deployed.instances[svcid].push({
		    'uuid': uuid,
		    'params': {
			'server_uuid': instance['cn']
		    },
		    'metadata': {
			'ZK_ID': byip[instance['ip']],
		        'DATACENTER': islocal ? localDc : remoteDc
		    }
		});
	});

	collector = new CollectorStream({});
	adm = new madm.MantaAdm(log);
	adm.loadFakeDeployed(deployed);
	problems = adm.dumpZkServers(collector, {});
	warnings = problems.critical.concat(problems.fixable);

	if (verbose) {
		console.error('output:');
		console.error(collector.data);
		console.error('warnings:');
		warnings.forEach(function (w) {
			console.error('    ', w.message);
		});
		console.error('');
	}

	/*
	 * Check for expected output.
	 */
	if (collector.data != testcase.output) {
		console.error('output mismatch! expected:');
		console.error(testcase.output);
		console.error('but found:');
		console.error(collector.data);
		callback(new VError('output mismatch'));
		return;
	}

	/*
	 * Now check for exactly the set of warnings that were expected.
	 */
	expected = testcase.warnings.slice(0);
	for (i = 0; i < expected.length; i++) {
		for (j = 0; j < warnings.length; j++) {
			if (expected[i].test(warnings[j].message)) {
				if (verbose) {
					console.error('found expected ' +
					    'warning: %s', expected[i]);
				}
				break;
			}
		}

		if (j == warnings.length) {
			callback(new VError(
			    'no match for expected warning: "%s"',
			    expected[i]));
			return;
		}

		warnings.splice(j, 1);
	}

	if (warnings.length > 0) {
		callback(new VError(
		    'unexpected warning: %s', warnings[0].message));
		return;
	}

	callback();
}

/*
 * Generate an identifier for CN "n".
 */
function identCn(n)
{
	return ('cn00' + n);
}

/*
 * Generate an identifier for VM "n".
 */
function identVm(n)
{
	return ('vm00' + n);
}


/*
 * Mainline
 */

var log = new bunyan({
    'name': 'tst.zk.js',
    'level': process.env['LOG_LEVEL'] || 'warn',
    'serializers': bunyan.stdSerializers
});

vasync.forEachPipeline({
    'func': runTestCase,
    'inputs': [ {
	'name': 'list zk servers, single DC',
	'zk_servers': [ '10.0.0.7', '10.0.0.8', '10.0.0.9' ],
	'cns': [ identCn(1), identCn(2), identCn(3) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.7'
	}, {
	    'cn': identCn(2),
	    'vm': identVm(2),
	    'ip': '10.0.0.8'
	}, {
	    'cn': identCn(3),
	    'vm': identVm(3),
	    'ip': '10.0.0.9'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(1), '10.0.0.7'),
	    outputRow(2, localDc, identVm(2), '10.0.0.8'),
	    outputRow(3, localDc, identVm(3), '10.0.0.9'),
	    ''
	].join('\n'),
	'warnings': []
    }, {
	'name': 'list zk servers, multi DC',
	'zk_servers': [ '10.0.0.7', '10.0.0.8', '10.0.0.9' ],
	'cns': [ identCn(1), identCn(3) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.7'
	}, {
	    'cn': identCn(2),
	    'vm': identVm(2),
	    'ip': '10.0.0.8'
	}, {
	    'cn': identCn(3),
	    'vm': identVm(3),
	    'ip': '10.0.0.9'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(1), '10.0.0.7'),
	    outputRow(2, remoteDc, identVm(2), '10.0.0.8'),
	    outputRow(3, localDc, identVm(3), '10.0.0.9'),
	    ''
	].join('\n'),
	'warnings': []
    }, {
	'name': 'list zk servers, single DC, entry missing from ZK_SERVERS',
	'zk_servers': [ '10.0.0.7', '10.0.0.9' ],
	'cns': [ identCn(1) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.7'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(2),
	    'ip': '10.0.0.8'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(3),
	    'ip': '10.0.0.9'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(1), '10.0.0.7'),
	    outputRow(2, localDc, identVm(3), '10.0.0.9'),
	    ''
	].join('\n'),
	'warnings': [ new RegExp('nameservice instance "vm002": ' +
	    'missing ZK_SERVERS entry') ]
    }, {
	'name': 'list zk servers, single DC, extra entry in ZK_SERVERS',
	'zk_servers': [ '10.0.0.7', '10.0.0.9', '10.0.0.11', '10.0.0.13' ],
	'cns': [ identCn(1) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.11'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(3),
	    'ip': '10.0.0.13'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(5),
	    'ip': '10.0.0.7'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(5), '10.0.0.7'),
	    outputRow(2, null, null, '10.0.0.9'),
	    outputRow(3, localDc, identVm(1), '10.0.0.11'),
	    outputRow(4, localDc, identVm(3), '10.0.0.13'),
	    ''
	].join('\n'),
	'warnings': [
	    new RegExp('ZK_SERVERS\\[1\\] has no associated SAPI instance')
	]
    }, {
	'name': 'list zk servers, multi DC, extra entry in ZK_SERVERS',
	'zk_servers': [ '10.0.0.7', '10.0.0.9', '10.0.0.11', '10.0.0.13',
	    '10.0.0.15' ],
	'cns': [ identCn(1) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.11'
	}, {
	    'cn': identCn(2),
	    'vm': identVm(7),
	    'ip': '10.0.0.9'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(3),
	    'ip': '10.0.0.13'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(5),
	    'ip': '10.0.0.7'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(5), '10.0.0.7'),
	    outputRow(2, remoteDc, identVm(7), '10.0.0.9'),
	    outputRow(3, localDc, identVm(1), '10.0.0.11'),
	    outputRow(4, localDc, identVm(3), '10.0.0.13'),
	    outputRow(5, null, null, '10.0.0.15'),
	    ''
	].join('\n'),
	'warnings': [
	    new RegExp('ZK_SERVERS\\[4\\] has no associated SAPI instance')
	]
    }, {
	'name': 'list zk servers, multi DC, local mismatch, count mismatch ' +
	    '(one missing server, definitely local)',
	'zk_servers': [ '10.0.0.7', '10.0.0.9', '10.0.0.11', '10.0.0.13' ],
	'cns': [ identCn(1) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.11'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(2),
	    'ip': '10.0.0.15'
	}, {
	    'cn': identCn(2),
	    'vm': identVm(3),
	    'ip': '10.0.0.9'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(4),
	    'ip': '10.0.0.13'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(5),
	    'ip': '10.0.0.7'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(5), '10.0.0.7'),
	    outputRow(2, remoteDc, identVm(3), '10.0.0.9'),
	    outputRow(3, localDc, identVm(1), '10.0.0.11'),
	    outputRow(4, localDc, identVm(4), '10.0.0.13'),
	    ''
	].join('\n'),
	'warnings': [
	    new RegExp('^nameservice instance "vm002": ' +
	        'missing ZK_SERVERS entry$')
	]
    }, {
	'name': 'list zk servers, multi DC, local mismatch, count match' +
	    ' (one missing local, one extra remote)',
	'zk_servers': [ '10.0.0.7', '10.0.0.9', '10.0.0.11', '10.0.0.13',
	    '10.0.0.17' ],
	'cns': [ identCn(1) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.11'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(2),
	    'ip': '10.0.0.15'
	}, {
	    'cn': identCn(2),
	    'vm': identVm(3),
	    'ip': '10.0.0.9'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(4),
	    'ip': '10.0.0.13'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(5),
	    'ip': '10.0.0.7'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(5), '10.0.0.7'),
	    outputRow(2, remoteDc, identVm(3), '10.0.0.9'),
	    outputRow(3, localDc, identVm(1), '10.0.0.11'),
	    outputRow(4, localDc, identVm(4), '10.0.0.13'),
	    outputRow(5, null, null, '10.0.0.17'),
	    ''
	].join('\n'),
	'warnings': [
	    new RegExp('^nameservice instance "vm002": ' +
	        'missing ZK_SERVERS entry$'),
	    new RegExp('^ZK_SERVERS\\[4\\] has no associated ' +
	        'SAPI instance')
	]
    }, {
	'name': 'list zk servers, multi DC, remote server missing',
	'zk_servers': [ '10.0.0.7', '10.0.0.9', '10.0.0.13', '10.0.0.15' ],
	'cns': [ identCn(1) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.7'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(2),
	    'ip': '10.0.0.9'
	}, {
	    'cn': identCn(2),
	    'vm': identVm(3),
	    'ip': '10.0.0.11'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(4),
	    'ip': '10.0.0.13'
	}, {
	    'cn': identCn(1),
	    'vm': identVm(5),
	    'ip': '10.0.0.15'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(1), '10.0.0.7'),
	    outputRow(2, localDc, identVm(2), '10.0.0.9'),
	    outputRow(3, localDc, identVm(4), '10.0.0.13'),
	    outputRow(4, localDc, identVm(5), '10.0.0.15'),
	    ''
	].join('\n'),
	'warnings': [
	    new RegExp('nameservice instance "vm003": missing ZK_SERVERS entry')
	]
    }, {
	'name': 'list zk servers, multi DC, local zone destroyed/missing',
	'zk_servers': [ '10.0.0.7', '10.0.0.8', '10.0.0.9' ],
	'cns': [ identCn(1), identCn(2) ],
	'instances': [ {
	    'cn': identCn(1),
	    'vm': identVm(1),
	    'ip': '10.0.0.7',
	    'zone_removed': true
	}, {
	    'cn': identCn(2),
	    'vm': identVm(2),
	    'ip': '10.0.0.8'
	}, {
	    'cn': identCn(3),
	    'vm': identVm(3),
	    'ip': '10.0.0.9'
	} ],
	'output': [
	    outputHeader,
	    outputRow(1, localDc, identVm(1), '10.0.0.7'),
	    outputRow(2, localDc, identVm(2), '10.0.0.8'),
	    outputRow(3, remoteDc, identVm(3), '10.0.0.9'),
	    ''
	].join('\n'),
	'warnings': [
	    new RegExp('nameservice instance "vm001": VM appears to have ' +
		'been provisioned in this datacenter, but could not be ' +
		'found in VMAPI')
	]
    } ]
}, function (err) {
	if (err)
		throw (err);
	console.log('TEST PASSED');
});
