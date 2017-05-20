/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.config.js: tests interfaces for iterating Amon probes and probe groups.
 * The test cases here do not make use of local metadata -- these just test the
 * basic interfaces for loading and iterating the objects stored in Amon.
 * These test cases work by implementing a mock Amon server, serving data that
 * comes from the test case specification, and then checking what we managed to
 * load from that server.
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var vasync = require('vasync');
var VError = require('verror');

var alarms = require('../../lib/alarms');
var mock_amon = require('./mock_amon');

var account = mock_amon.account;
var testCases = [];
var mockAmon;

function main()
{
	var log;

	log = new bunyan({
	    'name': 'tst.config.js',
	    'level': process.env['LOG_LEVEL'] || 'fatal',
	    'stream': process.stderr
	});

	mock_amon.createMockAmon(log, function (mock) {
		mockAmon = mock;

		/*
		 * Run the actual test cases.
		 */
		vasync.forEachPipeline({
		    'inputs': testCases,
		    'func': runTestCase
		}, function (err) {
			if (err) {
				throw (err);
			}

			mockAmon.server.close();
			console.log('%s okay', __filename);
		});
	});
}

/*
 * Execute a single test case.  Each test case specifies the set of Amon probe
 * groups and probes that should be returned.  This test runner uses the
 * interfaces that we're testing to load this data from a mock Amon server and
 * then calls onLoaded() to verify the results.  Unexpected failures (including
 * failures of test assertions) result in thrown exceptions.
 *
 * The test case object itself contains properties:
 *
 *     name		human-readable name of the test
 *
 *     components	names and types of each component whose probes should
 *     			be loaded from Amon.  See amonLoadComponentProbes().
 *
 *     amon		used to fill in the mock Amon server's current
 *     			configuration, which is used by the mock Amon server's
 *     			request handler.  This is how test cases specify which
 *     			probe groups and probes should be returned by the mock
 *     			Amon server.  See mockAmonHandleRequest() for details.
 *
 *     onLoaded		function to invoke after all this is done.  This accepts
 *     			arguments as onLoaded(pgerror, perror, pwarnings,
 *     			config), where:
 *
 *     		pgerror		an optional Error that's present if there
 *     				were any issues (either warnings or errors)
 *     				loading probe group information.  If these
 *     				issues were errors, then the "config" argument
 *     				will be null.
 *
 *     		perror		an optional Error that's present if there were
 *     				any errors loading probe information.  As long
 *     				as probe groups were loaded, "config" may still
 *     				be non-null, but may be missing some probe
 *     				information.
 *
 *     		pwarn		an optional Error that's present if there were
 *     				any warnings loading probe information.
 *
 *     		config		object that represents the loaded probe groups
 *     				and probes.  See amonLoadProbeGroups().  If this
 *     				object is present, then we successfully loaded
 *     				at least some probe groups.  If any of the other
 *     				arguments is present, then this configuration
 *     				may be incomplete because of the problems
 *     				described there.  As long as neither "pgerror"
 *     				nor "perror" is present, one can expect that
 *     				this configuration is complete enough to operate
 *     				on programmatically.
 */
function runTestCase(testcase, callback)
{
	console.log('test case: %s', testcase.name);
	assertplus.strictEqual(mockAmon.config, null);
	mockAmon.config = testcase.amon;

	alarms.amonLoadProbeGroups({
	    'amon': mockAmon.client,
	    'account': account
	}, function (lpgError, lpgConfig) {
		assertplus.equal(mockAmon.config, testcase.amon);

		if (!lpgConfig) {
			mockAmon.config = null;
			testcase.onLoaded(lpgError, null, null, null);
			callback();
			return;
		}

		alarms.amonLoadComponentProbes({
		    'amonRaw': mockAmon.clientRaw,
		    'amoncfg': lpgConfig,
		    'components': testcase.components,
		    'concurrency': 10
		}, function (lcpError, lcpWarnings) {
			assertplus.equal(mockAmon.config, testcase.amon);
			mockAmon.config = null;
			testcase.onLoaded(lpgError, lcpError,
			    lcpWarnings, lpgConfig);
			callback();
		});
	});
}

/*
 * Finally, define the actual test cases.
 */

testCases.push({
    'name': 'amon 500 error (listing probe groups)',
    'components': [],
    'amon': {
	'groups': 'error',
	'agentprobes': {}
    },
    'onLoaded': function (pgerr, perr, pwarn, config) {
	assertplus.ok(!perr);
	assertplus.ok(!pwarn);
	assertplus.ok(!config);
	assertplus.ok(pgerr);
	assertplus.ok(pgerr instanceof Error);
	assertplus.ok(/listing probegroups:.*injected error/.test(
	    pgerr.message));
    }
});

testCases.push({
    'name': 'amon 500 error (listing agent probes)',
    'components': [ { 'type': 'cn', 'uuid': 'c1' } ],
    'amon': {
	'groups': [],
	'agentprobes': { 'c1': 'error', 'c2': [] }
    },
    'onLoaded': function (pgerr, perr, pwarn, config) {
	assertplus.ok(!pgerr);
	assertplus.ok(!pwarn);
	assertplus.ok(config);
	assertplus.ok(perr);
	assertplus.ok(perr instanceof Error);
	assertplus.ok(
	    /* JSSTYLED */
	    /fetching probes for agent on cn "c1":.*injected error/.test(
	    perr.message));
    }
});

testCases.push({
    'name': 'empty configuration',
    'components': [ { 'type': 'cn', 'uuid': 'c1' } ],
    'amon': {
	'groups': [],
	'agentprobes': { 'c1': [] }
    },
    'onLoaded': function (pgerr, perr, pwarn, config) {
	assertplus.ok(!pgerr);
	assertplus.ok(!perr);
	assertplus.ok(!pwarn);
	assertplus.ok(config);
	assertplus.ok(!config.hasProbeGroup('foobar'));
	assertplus.strictEqual(config.probeGroupNameForUuid('foobar'), null);
	assertplus.strictEqual(config.probeGroupForName('foobar'), null);

	config.eachProbeGroup(function () {
		throw (new Error('unexpected probe group found'));
	});

	config.eachOrphanProbe(function () {
		throw (new Error('unexpected orphan probe found'));
	});

	assertplus.throws(function () {
	    config.eachProbeGroupProbe('foo', function () {});
	}, /unknown probe group name/);
    }
});

/*
 * This test case exercises the works: probes on both VMs and CNs, probes with
 * no probe groups, invalid probes and probe groups, probe groups with duplicate
 * names, and more.
 */
testCases.push({
    'name': 'complex configuration',
    'components': [
	{ 'type': 'cn', 'uuid': 'cn1' },
	{ 'type': 'vm', 'uuid': 'vm1' }
    ],
    'amon': {
	'groups': [ {
	    'uuid': 'probe-group-1',
	    'name': 'group1',
	    'user': account,
	    'disabled': false,
	    'contacts': [ 'contact1' ]
	}, {
	    'uuid': 'probe-group-2',
	    'name': 'group2',
	    'user': account,
	    'disabled': false,
	    'contacts': [ 'contact2' ]
	}, {
	    /* exercise warning case: duplicate probe group uuid */
	    'uuid': 'probe-group-2',
	    'name': 'group3',
	    'user': account,
	    'disabled': false,
	    'contacts': [ 'contact2' ]
	}, {
	    /* exercise warning case: totally invalid probe group */
	}, {
	    /* exercise warning case: invalid probe group having a uuid */
	    'uuid': 'probe-group-bogus'
	}, {
	    /* exercise warning case: duplicate probe group name */
	    'uuid': 'probe-group-3',
	    'name': 'group2',
	    'user': account,
	    'disabled': false,
	    'contacts': []
	} ],

	'agentprobes': {
	    'cn1': [ {
		'uuid': 'probe-uuid-1',
		'name': 'cn1-group1-probe1',
		'group': 'probe-group-1',
		'user': account,
		'type': 'cmd',
		'config': {},
		'agent': 'cn1',
		'machine': 'cn1',
		'groupEvents': true
	    }, {
		'uuid': 'probe-uuid-2',
		'name': 'cn1-group2-probe1',
		'group': 'probe-group-2',
		'user': account,
		'type': 'cmd',
		'config': {},
		'agent': 'cn1',
		'machine': 'cn1',
		'groupEvents': true
	    }, {
		/* exercise warning case: probe for unknown probe group */
		'uuid': 'probe-uuid-3',
		'name': 'cn1-group-unknown-probe1',
		'group': 'probe-group-unknown',
		'user': account,
		'type': 'cmd',
		'config': {},
		'agent': 'cn1',
		'machine': 'cn1',
		'groupEvents': true
	    }, {
		/* exercise warning case: totally invalid probe */
	    } ],
	    'vm1': [ {
		'uuid': 'probe-uuid-4',
		'name': 'vm1-group2-probe1',
		'group': 'probe-group-2',
		'user': account,
		'type': 'cmd',
		'config': {},
		'agent': 'vm1',
		'machine': 'vm1',
		'groupEvents': true
	    }, {
		/* exercise case of probe with no probe group */
		'uuid': 'probe-uuid-5',
		'name': 'vm1-nogroup-probe1',
		'user': account,
		'type': 'cmd',
		'config': {},
		'agent': 'vm1',
		'machine': 'vm1',
		'group': null,
		'groupEvents': true
	    } ]
	}
    },

    'onLoaded': function (pgerr, perr, pwarn, config) {
	var warnings, pg, found;

	/*
	 * We'll have a bunch of warnings, but we should end up with a valid
	 * configuration.  Group all the warnings, sort them by message, and
	 * check each of the messages.
	 */
	assertplus.ok(config);
	assertplus.ok(!perr);

	warnings = [];
	VError.errorForEach(pgerr, function (e) { warnings.push(e.message); });
	VError.errorForEach(pwarn, function (e) { warnings.push(e.message); });
	warnings = warnings.sort();
	assertplus.deepEqual(warnings, [
	    'ignoring group: duplicate probe group name: "group2"',
	    'ignoring group: duplicate probe group uuid: "probe-group-2"',
	    'ignoring group: probe group "probe-group-bogus": ' +
		'property "name": is missing and it is required',
	    'ignoring group: property "uuid": is missing and it is required',
	    'ignoring probe: probe "probe-uuid-3": unknown probe group ' +
	        '"probe-group-unknown"',
	    'ignoring probe: property "type": is missing and it is required'
	]);

	/*
	 * Test hasProbeGroup().
	 */
	assertplus.ok(config.hasProbeGroup('group1'));
	assertplus.ok(config.hasProbeGroup('group2'));
	assertplus.ok(!config.hasProbeGroup('group3'));

	/*
	 * Test probeGroupNameForUuid() and probeGroupForName().
	 */
	assertplus.strictEqual(
	    config.probeGroupNameForUuid('probe-group-1'), 'group1');
	pg = config.probeGroupForName('group1');
	assertplus.notStrictEqual(pg, null);
	assertplus.equal(pg.pg_name, 'group1');
	assertplus.equal(pg.pg_uuid, 'probe-group-1');

	/* The group with a duplicate name should not be present. */
	assertplus.strictEqual(
	    config.probeGroupNameForUuid('probe-group-3'), null);

	/*
	 * Test eachProbeGroup().
	 */
	found = [];
	config.eachProbeGroup(function (fpg) {
		found.push(fpg);
	});

	found = found.sort(function (pg1, pg2) {
		return (pg1.pg_uuid.localeCompare(pg2.pg_uuid));
	});

	assertplus.equal(found.length, 2);
	assertplus.ok(pg == found[0]);
	assertplus.equal(found[1].pg_uuid, 'probe-group-2');

	/*
	 * Test eachProbeGroupProbe().
	 */
	found = [];
	config.eachProbeGroupProbe('group2', function (p) {
		assertplus.equal(p.p_groupid, 'probe-group-2');
		found.push(p.p_uuid);
	});
	found = found.sort();
	assertplus.deepEqual(found, [ 'probe-uuid-2', 'probe-uuid-4' ]);

	/*
	 * Test eachOrphanProbe().
	 */
	found = [];
	config.eachOrphanProbe(function (p) {
		found.push(p.p_uuid);
	});
	assertplus.deepEqual(found, [ 'probe-uuid-5' ]);
    }
});

testCases.push(makeBigTestCase());

/*
 * Creates a test case that exercises a fairly large configuration.  In
 * practice, we expect the number of probe groups to remain reasonably bounded
 * (on the order of 100), but the number of other objects might scale a bunch
 * higher: CNs (order of 1000) and VMs (order of 1000 per box).  We're not near
 * these numbers yet, and we'll likely need better interfaces for working with
 * these objects at that scale.  For now, we set these to be a bit higher than
 * we expect to see in production.
 */
function makeBigTestCase()
{
	var ncns = 150;
	var nvmspercn = 15;
	var ngroups = 50;
	var scaleTestCase = {
	    'name': 'very large config',
	    'components': [],
	    'amon': {
		'groups': [],
		'agentprobes': {}
	    },
	    'onLoaded': function (pgerr, perr, pwarn, config) {
		var nfound;

		assertplus.ok(!pgerr);
		assertplus.ok(!perr);
		assertplus.ok(!pwarn);

		/*
		 * Sanity-check the configuration.
		 */
		assertplus.ok(config.hasProbeGroup('group-0-name'));
		assertplus.ok(!config.hasProbeGroup(
		    'group-' + ngroups + '-name'));

		/*
		 * Iterate and count the probe groups.
		 */
		nfound = 0;
		config.eachProbeGroup(function (pg) { nfound++; });
		assertplus.equal(nfound, ngroups);

		/*
		 * Iterate and count the probes.
		 */
		nfound = 0;
		config.eachProbeGroup(function (pg) {
			config.eachProbeGroupProbe(pg.pg_name, function (p) {
				nfound++;
			});
		});
		assertplus.equal(nfound, ngroups * ncns * (nvmspercn + 1));
	    }
	};

	var cni, vmi, groupi;
	var cnname, vmname, gname, name, probes, nprobes;

	nprobes = 0;
	process.stdout.write('generating test case ... ');

	for (groupi = 0; groupi < ngroups; groupi++) {
		scaleTestCase.amon.groups.push({
		    'uuid': 'group-' + groupi + '-uuid',
		    'name': 'group-' + groupi + '-name',
		    'user': account,
		    'disabled': false,
		    'contacts': [ 'contact1' ]
		});
	}

	for (cni = 0; cni < ncns; cni++) {
		cnname = 'cn' + cni;
		scaleTestCase.components.push({
		    'type': 'cn',
		    'uuid': cnname
		});

		for (vmi = 0; vmi < nvmspercn; vmi++) {
			vmname = cnname + '-vm-' + vmi;
			scaleTestCase.components.push({
			    'type': 'vm',
			    'uuid': vmname
			});
		}

		probes = scaleTestCase.amon.agentprobes[cnname] = [];
		for (groupi = 0; groupi < ngroups; groupi++) {
			gname = 'group-' + groupi;
			name = cnname + '-group-' + groupi + '-probe';
			probes.push({
			    'uuid': name + '-uuid',
			    'name': name + '-name',
			    'group': gname + '-uuid',
			    'user': account,
			    'type': 'cmd',
			    'config': {},
			    'agent': cnname,
			    'machine': vmname,
			    'groupEvents': true
			});
			nprobes++;
		}

		for (vmi = 0; vmi < nvmspercn; vmi++) {
			vmname = cnname + '-vm-' + vmi;
			probes = scaleTestCase.amon.agentprobes[vmname] = [];
			for (groupi = 0; groupi < ngroups; groupi++) {
				gname = 'group-' + groupi;
				name = vmname + '-group-' + groupi + '-probe';
				probes.push({
				    'uuid': name + '-uuid',
				    'name': name + '-name',
				    'group': gname + '-uuid',
				    'user': account,
				    'type': 'cmd',
				    'config': {},
				    'agent': vmname,
				    'machine': vmname,
				    'groupEvents': true
				});
				nprobes++;
			}
		}
	}

	console.log('%d probes', nprobes);
	return (scaleTestCase);
}

main();
