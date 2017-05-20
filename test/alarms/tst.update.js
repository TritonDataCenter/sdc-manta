/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.update.js: tests facilities used to update a the deployed probes and
 * probe groups.
 */

var assertplus = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var sprintf = require('extsprintf').sprintf;
var vasync = require('vasync');

var alarms = require('../../lib/alarms');
var alarm_metadata = require('../../lib/alarms/metadata');
var instance_info = require('../../lib/instance_info');
var mock_amon = require('./mock_amon');
var services = require('../../lib/services');

/*
 * These test cases are somewhat tedious because in order to verify that the
 * software is doing what we expect, we need to enumerate the expected probes
 * and probe groups for various cases.  But there are also a lot of different
 * cases to test:
 *
 *   - configuring and unconfiguring
 *   - scopes that are per-service, for each service, or for all services
 *   - global scopes
 *   - cases where there are no instances of a service in this datacenter
 *   - groups and probes that were operator-added
 *   - groups and probes added by previous versions of the software
 *   - cases where no changes need to be made
 *   - cases where partial changes need to be made
 *
 * The goal is to exercise a set of test cases, each of which describes:
 *
 *   - a set of CNs and VMs in a datacenter
 *   - a set of probe metadata (like the one shipped with this repository, but
 *     much simpler)
 *   - a set of probes and probe groups that are deployed already
 *   - whether the test operation is a "configure" or "unconfigure" operation
 *
 * For each test case, we will generate an Amon update plan according to the
 * parameters of the test case and verify its basic parameters (mostly: the
 * numbers of groups and probes added and removed).
 *
 * We do this in a few stages:
 *
 *   (1) generateTestDatacenters() generates descriptions of a handful of
 *       datacenters that we'll use for testing:
 *
 *       - "empty": a single, empty datacenter (degenerate case)
 *       - "single": a single-datacenter case that we use to exercise most of
 *         the cases
 *       - "multi": a multi-datacenter case that we use only to sanity-test that
 *         case
 *
 *   (2) generateTestMetadata() uses hardcoded metadata descriptions to load
 *       probe metadata that exercises the various cases that we care about
 *
 *   (3) generateMockAmonObjects() uses the mock Amon server in this directory
 *       to load sample hardcoded "deployed" probes and probe groups
 *
 *   (4) generateTestCases() generates the actual test cases, which are
 *       expressed in terms of the above (a description of the datacenter we're
 *       testing, the probe metadata to use, the set of groups and probes to
 *       pretend are deployed, etc.)
 *
 * Finally, we run through all the test cases, invoking the verifier function to
 * assert whatever the test case needs about the resulting plan.
 */
var testCases;

/*
 * Datacenter configurations
 *
 * See above.  This structure is filled in by generateTestDatacenters().
 */
var dcconfigs = {
    'cfg_empty': new DatacenterConfig(),
    'cfg_basic': new DatacenterConfig(),
    'cfg_multi': new DatacenterConfig()
};

/*
 * nInstancesBySvc is used to by generateTestDatacenters() to generate a list of
 * fake VMs and CNs for a datacenter.  The only reason that the code we're
 * testing isn't completely agnostic to service names is because the
 * implementation of the "all" scope requires knowing all of the service names.
 * We don't actually need to exercise every service differently, and doing so
 * would be pretty tedious because we'll need to manually list out all of the
 * expected probes for every instance.  So we only define a few instances and
 * test out those.
 */
var nInstancesBySvc = {
    'nameservice': 3,
    'jobsupervisor': 2
};

/*
 * Parameters used for all of the tests.
 */
var account = mock_amon.account;
var contactsBySeverity = {
    'minor': [ 'minor_contact1', 'minor_contact2', 'minor_contact3' ],
    'major': [ 'major_contact' ],
    'critical': [ 'critical_contact1', 'critical_contact2' ]
};

/*
 * Probe metadata
 *
 * The raw objects below are processed using the normal metadata loading code to
 * generate loaded versions.  We test a couple of different metadata
 * configurations: one with no metadata (as a degenerate case) and another one
 * that covers a bunch of the cases described above.
 */

/* loaded versions */
var metadataEmpty, metadataBasic;

/* raw versions (JSON representations of the metadata) */
var rawMetadataEmpty = [];
var rawMetadataBasic = [ {
    /* scope: basic service scope */
    'event': 'upset.manta.test.nameservice_broken',
    'scope': { 'service': 'nameservice' },
    'checks': [ {
	'type': 'cmd',
	'config': {
	    'env': { 'complex': 'snpp' },
	    'autoEnv': [ 'sector' ]
	}
    } ],
    'ka': {
	'title': 'test ka: basic "service" scope',
	'description': 'exercises a basic "service" scope template',
	'severity': 'minor',
	'response': 'none',
	'impact': 'none',
	'action': 'none'
    }
}, {
    /* scope: "global" */
    'event': 'upset.manta.test.global',
    'scope': { 'service': 'nameservice', 'global': true },
    'checks': [ { 'type': 'cmd', 'config': {} } ],
    'ka': {
	'title': 'test ka: global "service" scope',
	'description': 'exercises a global "service" scope template',
	'severity': 'major',
	'response': 'none',
	'impact': 'none',
	'action': 'none'
    }
}, {
    /* scope: "each" */
    'event': 'upset.manta.test.$service',
    'scope': { 'service': 'each' },
    'checks': [ { 'type': 'cmd', 'config': {} } ],
    'ka': {
	'title': 'test ka: each "service" scope',
	'description': 'exercises an "each" "service" scope template',
	'severity': 'critical',
	'response': 'none',
	'impact': 'none',
	'action': 'none'
    }
}, {
    /* scope: "all" */
    'event': 'upset.manta.test.all',
    'scope': { 'service': 'all' },
    'checks': [ { 'type': 'cmd', 'config': {} } ],
    'ka': {
	'title': 'test ka: all "service" scope',
	'description': 'exercises an "all" "service" scope template',
	'severity': 'minor',
	'response': 'none',
	'impact': 'none',
	'action': 'none'
    }
} ];

/*
 * List of probe groups that should be deployed by the above metadata,
 * regardless of the datacenter configuration.
 */
var deployedGroups = [ {
    'uuid': 'deployed-group-uuid-1',
    'name': 'upset.manta.test.nameservice_broken;v=1',
    'user': account,
    'disabled': false,
    'contacts': contactsBySeverity.minor
}, {
    'uuid': 'deployed-group-uuid-2',
    'name': 'upset.manta.test.global;v=1',
    'user': account,
    'disabled': false,
    'contacts': contactsBySeverity.major
}, {
    'uuid': 'deployed-group-uuid-3',
    'name': 'upset.manta.test.all;v=1',
    'user': account,
    'disabled': false,
    'contacts': contactsBySeverity.minor
} ];

function main()
{
	var log;

	log = new bunyan({
	    'name': 'tst.update.js',
	    'level': process.env['LOG_LEVEL'] || 'fatal',
	    'stream': process.stderr
	});

	generateTestDatacenters();
	generateTestMetadata();
	mock_amon.createMockAmon(log, function (mock) {
		generateMockAmonObjects(mock, function () {
			mock.server.close();
			generateTestCases();
			testCases.forEach(runTestCase);
			console.log('%s okay', __filename);
		});
	});
}

/*
 * Populates "dcconfigs" with a reasonable set of CNs and VMs for each of the
 * three configurations we intend to test.
 */
function generateTestDatacenters()
{
	var instances, instancesBySvc, servernames;

	/*
	 * The empty DC is easy: just set up the data structure.
	 */
	jsprim.forEachKey(nInstancesBySvc, function (svcname, n) {
		dcconfigs.cfg_empty.ctp_instances_by_svcname[
		    svcname] = [];
	});

	/*
	 * For the basic single datacenter case, fake up instances in numbers
	 * described by nInstancesBySvc.
	 */
	instances = dcconfigs.cfg_basic.ctp_instances;
	instancesBySvc = dcconfigs.cfg_basic.ctp_instances_by_svcname;
	servernames = {};
	jsprim.forEachKey(nInstancesBySvc, function (svcname, n) {
		var i, instid, cnname;
		instancesBySvc[svcname] = [];
		for (i = 0; i < n; i++) {
			instid = sprintf('svc-%s-%d', svcname, i);
			/*
			 * We use only two different CN uuids to make sure that
			 * we re-use CNs for a given service.  That's in order
			 * to make sure that "global" scoped templates do not
			 * generate multiple probes for the same CN just because
			 * there are two instances of a service on that CN.
			 */
			cnname = sprintf('server-uuid-%d', i % 2);
			instancesBySvc[svcname].push(instid);
			instances[instid] = new instance_info.InstanceInfo({
			    'uuid': instid,
			    'svcname': svcname,
			    'server_uuid': cnname,
			    'local': true,
			    'metadata': {
				'sector': '7G'
			    }
			});

			servernames[cnname] = true;
		}
	});
	dcconfigs.cfg_basic.ctp_servers = Object.keys(servernames);
	dcconfigs.cfg_basic.ctp_vms_destroyed = [ 'destroyed-nameservice' ];
	dcconfigs.cfg_basic.ctp_servers_abandoned = [ 'abandoned-cn' ];

	/*
	 * For the multi-datacenter case, we need to fake up information about
	 * instances in all three DCs.
	 */
	instances = dcconfigs.cfg_multi.ctp_instances;
	instancesBySvc = dcconfigs.cfg_multi.
	    ctp_instances_by_svcname;
	servernames = {};
	jsprim.forEachKey(nInstancesBySvc, function (svcname, n) {
		var i, instid, cnname, iiargs;

		instancesBySvc[svcname] = [];

		/*
		 * This is a quick way of spreading instances across the
		 * datacenter.  It's not exactly how we'd really do it, but it
		 * should be close enough for our purposes.
		 */
		for (i = 0; i < n; i++) {
			instid = sprintf('dc%d-%s-inst%d',
			    i % 3, svcname, i);
			iiargs = {
			    'uuid': instid,
			    'svcname': svcname,
			    'metadata': {
				'sector': '7G'
			    }
			};

			/*
			 * Since we're already ignoring most services for the
			 * purpose of this test case, we know that there will be
			 * many services with no instances in the local
			 * datacenter.
			 */
			if (i % 3 === 0) {
				cnname = sprintf('server-uuid-%d', i);
				instancesBySvc[svcname].push(instid);
				iiargs['local'] = true;
				iiargs['server_uuid'] = cnname;
				servernames[cnname] = true;
			} else {
				iiargs['local'] = false;
				iiargs['server_uuid'] = null;
			}

			instances[instid] = new instance_info.InstanceInfo(
			    iiargs);
		}
	});
	dcconfigs.cfg_multi.ctp_servers = Object.keys(servernames);
}

/*
 * Load the raw metadata (hardcoded above).
 */
function generateTestMetadata()
{
	var mdl, errors;

	mdl = new alarm_metadata.MetadataLoader();
	mdl.loadFromString(JSON.stringify(rawMetadataEmpty), 'input');
	errors = mdl.errors();
	assertplus.strictEqual(errors.length, 0);
	metadataEmpty = mdl.mdl_amoncfg;

	mdl = new alarm_metadata.MetadataLoader();
	mdl.loadFromString(JSON.stringify(rawMetadataBasic), 'input');
	errors = mdl.errors();
	assertplus.strictEqual(errors.length, 0);
	metadataBasic = mdl.mdl_amoncfg;
}

/*
 * Use the mock Amon server to load various combinations of probes and probe
 * groups.  Unfortunately, these are extremely datacenter-specific and
 * metadata-specific, so there's not a great way to avoid hardcoding a bunch of
 * different combinations of deployed groups and probes.
 */
function generateMockAmonObjects(mock, callback)
{
	mock.config = {};
	mock.config.groups = [];
	mock.config.agentprobes = {};

	vasync.waterfall([
		/*
		 * Generate a config representing no probes deployed to the
		 * empty DC configuration.
		 */
		function emptyDcNoProbes(subcallback) {
			var dc = dcconfigs.cfg_empty;
			loadDeployedForConfig(mock, dc, function (cfg) {
				dc.ctp_deployed_none = cfg;
				subcallback();
			});
		},

		/*
		 * Generate a config representing no probes deployed to the
		 * basic single-DC configuration.
		 */
		function basicDcNoProbes(subcallback) {
			var dc = dcconfigs.cfg_basic;
			loadDeployedForConfig(mock, dc, function (cfg) {
				dc.ctp_deployed_none = cfg;
				subcallback();
			});
		},

		/*
		 * Generate a config representing no probes deployed to the
		 * multi-DC configuration.
		 */
		function multiDcNoProbes(subcallback) {
			var dc = dcconfigs.cfg_multi;
			loadDeployedForConfig(mock, dc, function (cfg) {
				dc.ctp_deployed_none = cfg;
				subcallback();
			});
		},

		/*
		 * Generate a config representing all of the expected probes
		 * deployed to the basic single-DC configuration.
		 */
		function basicDcFullProbes(subcallback) {
			var dc = dcconfigs.cfg_basic;

			/*
			 * Start with the hardcoded groups and add a group for
			 * each service that supports probes to reflect the
			 * "each" probe template.
			 */
			mock.config.groups = jsprim.deepCopy(deployedGroups);
			services.mSvcNamesProbes.forEach(function (svcname, i) {
				svcname = svcname.replace(/-/g, '_');
				mock.config.groups.push({
				    'uuid': 'deployed-group-uuid-svc-' +
				        svcname,
				    'name': 'upset.manta.test.' + svcname +
				        ';v=1',
				    'user': account,
				    'disabled': false,
				    'contacts': contactsBySeverity.critical
				});
			});

			/*
			 * Define probes for each of the local instances for
			 * each of the groups above.
			 *
			 * Specifying the environment here implicitly tests the
			 * behavior of the "autoEnv" property, since if it
			 * didn't work, the software would make additional
			 * changes to the deployed probes.
			 */
			mock.config.agentprobes = {};
			mock.config.agentprobes['svc-nameservice-0'] = [
			    makeProbe({
			        'group': 'deployed-group-uuid-1',
				'name': 'upset.manta.test.nameservice_broken0',
				'agent': 'svc-nameservice-0',
				'config': {
				    'env': {
					'complex': 'snpp',
					'sector': '7G'
				    }
				}
			    }),
			    makeProbe({
				'group': 'deployed-group-uuid-3',
				'name': 'upset.manta.test.all0',
				'agent': 'svc-nameservice-0'
			    }),
			    makeProbe({
			        'group': 'deployed-group-uuid-svc-nameservice',
				'name': 'upset.manta.test.nameservice0',
				'agent': 'svc-nameservice-0'
			    })
			];
			mock.config.agentprobes['svc-nameservice-1'] = [
			    makeProbe({
			        'group': 'deployed-group-uuid-1',
				'name': 'upset.manta.test.nameservice_broken0',
				'agent': 'svc-nameservice-1',
				'config': {
				    'env': {
					'complex': 'snpp',
					'sector': '7G'
				    }
				}
			    }),
			    makeProbe({
				'group': 'deployed-group-uuid-3',
				'name': 'upset.manta.test.all0',
				'agent': 'svc-nameservice-1'
			    }),
			    makeProbe({
			        'group': 'deployed-group-uuid-svc-nameservice',
				'name': 'upset.manta.test.nameservice0',
				'agent': 'svc-nameservice-1'
			    })
			];
			mock.config.agentprobes['svc-nameservice-2'] = [
			    makeProbe({
			        'group': 'deployed-group-uuid-1',
				'name': 'upset.manta.test.nameservice_broken0',
				'agent': 'svc-nameservice-2',
				'config': {
				    'env': {
					'complex': 'snpp',
					'sector': '7G'
				    }
				}
			    }),
			    makeProbe({
				'group': 'deployed-group-uuid-3',
				'name': 'upset.manta.test.all0',
				'agent': 'svc-nameservice-2'
			    }),
			    makeProbe({
			        'group': 'deployed-group-uuid-svc-nameservice',
				'name': 'upset.manta.test.nameservice0',
				'agent': 'svc-nameservice-2'
			    })
			];

			mock.config.agentprobes['svc-jobsupervisor-0'] = [
			    makeProbe({
				'group': 'deployed-group-uuid-3',
				'name': 'upset.manta.test.all0',
				'agent': 'svc-jobsupervisor-0'
			    }),
			    makeProbe({
				'group': 'deployed-group-uuid-svc-' +
				    'jobsupervisor',
				'name': 'upset.manta.test.jobsupervisor0',
				'agent': 'svc-jobsupervisor-0'
			    })
			];
			mock.config.agentprobes['svc-jobsupervisor-1'] = [
			    makeProbe({
				'group': 'deployed-group-uuid-3',
				'name': 'upset.manta.test.all0',
				'agent': 'svc-jobsupervisor-1'
			    }),
			    makeProbe({
				'group': 'deployed-group-uuid-svc-' +
				     'jobsupervisor',
				'name': 'upset.manta.test.jobsupervisor0',
				'agent': 'svc-jobsupervisor-1'
			    })
			];

			mock.config.agentprobes['server-uuid-0'] = [
			    makeProbe({
				'group': 'deployed-group-uuid-2',
				'name': 'upset.manta.test.global0',
				'agent': 'server-uuid-0'
			    })
			];
			mock.config.agentprobes['server-uuid-1'] = [
			    makeProbe({
				'group': 'deployed-group-uuid-2',
				'name': 'upset.manta.test.global0',
				'agent': 'server-uuid-1'
			    })
			];

			loadDeployedForConfig(mock, dc, function (cfg) {
				dc.ctp_deployed_full = cfg;
				subcallback();
			});
		},

		/*
		 * To the previous configuration, add the legacy probe group,
		 * the operator-created probe group, the probe group from a
		 * future version, the probes for these, the probe that has no
		 * probe group, and the probe that has a group that doesn't
		 * exist.
		 */
		function basicDcExtraProbes(subcallback) {
			var dc = dcconfigs.cfg_basic;
			var nsagent = dc.ctp_instances_by_svcname[
			    'nameservice'][0];

			mock.config.groups.push({
			    'uuid': 'operator-group-1',
			    'name': 'operator-created group 1',
			    'user': account,
			    'disabled': false,
			    'contacts': [ 'operator-contact-1' ]
			});
			mock.config.groups.push({
			    'uuid': 'future-group-1',
			    'name': 'upset.manta.future;v=2',
			    'user': account,
			    'disabled': false,
			    'contacts': [ 'operator-contact-2' ]
			});
			mock.config.groups.push({
			    'uuid': 'nameservice-alert-uuid',
			    'name': 'nameservice-alert',
			    'user': account,
			    'disabled': false,
			    'contacts': [ 'major_contact' ]
			});

			/* probe for the operator's custom group */
			mock.config.agentprobes['server-uuid-0'].push(
			    makeProbe({
			        'name': 'operator-1',
			        'group': 'operator-group-1'
			    }));
			/* probe from the future */
			mock.config.agentprobes['server-uuid-0'].push(
			    makeProbe({
			        'name': 'future-1',
				'group': 'future-group-1'
			    }));
			/* probe having no group at all */
			mock.config.agentprobes['server-uuid-0'].push(
			    makeProbe({
				'name': 'rogue'
			    }));
			/* probe for group that's missing */
			mock.config.agentprobes['server-uuid-0'].push(
			    makeProbe({
				'name': 'badgroup',
				'group': 'no-such-group'
			    }));
			/* probe for the nameservice's legacy group */
			mock.config.agentprobes[nsagent].push(makeProbe({
			    'name': 'nameservice-legacy',
			    'group': 'nameservice-alert-uuid',
			    'agent': nsagent
			}));

			loadDeployedForConfig(mock, dc, function (cfg) {
				dc.ctp_deployed_extra = cfg;
				subcallback();
			});
		},

		/*
		 * From the previous configuration, remove some of the probes
		 * that we would normally deploy automatically.  This represents
		 * a partially deployed configuration and tests that the
		 * software does the right thing for smaller, incremental
		 * updates.
		 *
		 * We also add a probe for an existing, automatically-managed
		 * probe group and a zone that no longer exists to test that we
		 * clean these up.
		 */
		function basicDcPartialProbes(subcallback) {
			var dc = dcconfigs.cfg_basic;
			var groupToRm = 'deployed-group-uuid-svc-nameservice';
			var nsagent = dc.ctp_instances_by_svcname[
			    'nameservice'][0];

			/*
			 * Remove one of the deployed probe groups and its
			 * probes.
			 */
			mock.config.groups = mock.config.groups.filter(
			    function (g) {
				return (g.uuid != groupToRm);
			    });
			jsprim.forEachKey(mock.config.agentprobes,
			    function (agentuuid, agentprobes) {
				if (dc.ctp_instances_by_svcname[
				    'nameservice'].indexOf(agentuuid) == -1) {
					return;
				}

				mock.config.agentprobes[agentuuid] =
				    agentprobes.filter(function (p) {
					return (p.group != groupToRm);
				    });
			    });

			/*
			 * Remove one of the deployed probes for another probe
			 * group.
			 */
			mock.config.agentprobes[nsagent] =
			    mock.config.agentprobes[nsagent].filter(
			    function (p) {
				return (p.group != 'deployed-group-uuid-1');
			    });

			/*
			 * Create probes for destroyed VMs and for a CN
			 * associated with a destroyed VM.
			 */
			mock.config.agentprobes['destroyed-nameservice'] = [
			    makeProbe({
			        'group': 'deployed-group-uuid-1',
				'name': 'upset.manta.test.nameservice_broken0',
				'agent': 'destroyed-nameservice'
			    })
			];

			mock.config.agentprobes['abandoned-cn'] = [
			    makeProbe({
				'group': 'deployed-group-uuid-2',
				'name': 'upset.manta.test.global0',
				'agent': 'abandoned-cn'
			    })
			];

			loadDeployedForConfig(mock, dc, function (cfg) {
				dc.ctp_deployed_partial = cfg;
				subcallback();
			});
		}
	], function (err) {
		assertplus.ok(!err);
		callback();
	});
}

/*
 * Given the mock Amon configuration and the specified datacenter configuraiton,
 * load the set of deployed probe groups and probes.
 */
function loadDeployedForConfig(mock, dcconfig, callback)
{
	var components;

	/*
	 * We use the datacenter description to assemble a list of components
	 * for which to fetch probes and then defer to the usual code path to
	 * actually load the objects.
	 */
	assertplus.object(dcconfig.ctp_servers);
	components = [];
	dcconfig.ctp_servers.forEach(function (s) {
		components.push({ 'type': 'cn', 'uuid': s });
	});
	jsprim.forEachKey(dcconfig.ctp_instances, function (_, instance) {
		if (!instance.inst_local) {
			return;
		}

		components.push({ 'type': 'vm', 'uuid': instance.inst_uuid });
	});
	dcconfig.ctp_vms_destroyed.forEach(function (uuid) {
		components.push({ 'type': 'vm', 'uuid': uuid });
	});
	dcconfig.ctp_servers_abandoned.forEach(function (s) {
		components.push({ 'type': 'cn', 'uuid': s });
	});

	alarms.amonLoadProbeGroups({
	    'amon': mock.client,
	    'account': account
	}, function (err, config) {
		assertplus.ok(!err);
		assertplus.ok(config);
		alarms.amonLoadComponentProbes({
		    'amonRaw': mock.clientRaw,
		    'amoncfg': config,
		    'components': components,
		    'concurrency': 3
		}, function (probeError) {
			assertplus.ok(!probeError);
			callback(config);
		});
	});
}

function generateTestCases()
{
	var ngroupsfull, nprobesfull, nprobesmulti;

	testCases = [];

	/*
	 * There are three non-"each" templates, plus an "each" template
	 * that generates a group for each service that supports probes.
	 */
	ngroupsfull = deployedGroups.length + services.mSvcNamesProbes.length;

	/*
	 * For the single-DC case, we've got:
	 *
	 *   - 3 "nameservice" probes for the "nameservice" template
	 *   - 2 "global" probes for the "global" template
	 *   - 3 "nameservice" probes for the "each" template
	 *   - 2 "jobsupervisor" probes for the "each" template
	 *   - 5 probes for the "all" template
	 *
	 * totalling 15 probes.
	 */
	nprobesfull = 15;

	/*
	 * For the multi-DC case, we've got:
	 *
	 *   - 1 "nameservice" probe for the "nameservice" template
	 *     (because other instances are in other DCs)
	 *   - 1 "global" probe for the "global" template
	 *     (again, because other nameservice instances are in other DCs)
	 *   - 1 "nameservice" probe for the "each" template
	 *   - 1 "jobsupevisor" probe for the "each" template
	 *   - 2 probes for the "all" template
	 */
	nprobesmulti = 6;

	testCases.push({
	    'name': 'empty DC, undeployed, configure with no metadata',
	    'metadata': metadataEmpty,
	    'dcConfig': dcconfigs.cfg_empty,
	    'deployed': 'none',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(!plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'empty DC, undeployed, configure (add groups only)',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_empty,
	    'deployed': 'none',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, ngroupsfull);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'empty DC, undeployed, unconfigure (no changes)',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_empty,
	    'deployed': 'none',
	    'unconfigure': true,
	    'verify': function (plan) {
		assertplus.ok(!plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, undeployed, configure with no metadata',
	    'metadata': metadataEmpty,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'none',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(!plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, undeployed, configure (many changes)',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'none',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, ngroupsfull);
		assertplus.strictEqual(plan.mup_probes_add.length, nprobesfull);
	    }
	});

	testCases.push({
	    'name': 'basic DC, undeployed, unconfigure (no changes)',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'none',
	    'unconfigure': true,
	    'verify': function (plan) {
		assertplus.ok(!plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, deployed, configure with no metadata',
	    'metadata': metadataEmpty,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'full',
	    'unconfigure': true,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length,
		    nprobesfull);
		assertplus.strictEqual(plan.mup_groups_remove.length,
		    ngroupsfull);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, deployed, configure (no changes)',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'full',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(!plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, deployed, unconfigure (many changes)',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'full',
	    'unconfigure': true,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		assertplus.strictEqual(plan.mup_probes_remove.length,
		    nprobesfull);
		assertplus.strictEqual(plan.mup_groups_remove.length,
		    ngroupsfull);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, deployed with extra, configure',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'extra',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		/* We expect to remove only the legacy group and probe. */
		assertplus.strictEqual(plan.mup_probes_remove.length, 1);
		assertplus.strictEqual(plan.mup_groups_remove.length, 1);
		/* We don't expect to add anything. */
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, deployed with extra, unconfigure',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'extra',
	    'unconfigure': true,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		/*
		 * We expect to remove the legacy group and probe, plus the
		 * usual ones.
		 */
		assertplus.strictEqual(plan.mup_probes_remove.length,
		    1 + nprobesfull);
		assertplus.strictEqual(plan.mup_groups_remove.length,
		    1 + ngroupsfull);
		/* We don't expect to add anything. */
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'basic DC, partially deployed, configure',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'partial',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());

		/*
		 * We expect to remove the legacy probe group, its probe,
		 * and the VM-level and CN-level probes for the VM that no
		 * longer exists and the CN that no longer hosts any VMs.
		 */
		assertplus.strictEqual(plan.mup_groups_remove.length, 1);
		assertplus.strictEqual(plan.mup_probes_remove.length, 3);

		/*
		 * We expect to add one group that was missing, plus one probe
		 * for each nameservice for the group that was missing, plus
		 * one additional probe for the probe that was missing.
		 */
		assertplus.strictEqual(plan.mup_groups_add.length, 1);
		assertplus.strictEqual(plan.mup_probes_add.length, 4);
	    }
	});

	testCases.push({
	    'name': 'basic DC, partially deployed, unconfigure',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_basic,
	    'deployed': 'partial',
	    'unconfigure': true,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());

		/*
		 * We expect to remove all of the usual groups except for the
		 * one that was already missing (because this was a partial
		 * deployment to begin with), plus the legacy one.
		 */
		assertplus.strictEqual(plan.mup_groups_remove.length,
		    ngroupsfull);

		/*
		 * Similarly, we expect to remove all of the usual probes except
		 * for the four that were already missing (because this was a
		 * partial deployment), plus the legacy one, plus the one each
		 * for the VM and CN that no longer exist.
		 */
		assertplus.strictEqual(plan.mup_probes_remove.length,
		    nprobesfull - 1);
		assertplus.strictEqual(plan.mup_groups_add.length, 0);
		assertplus.strictEqual(plan.mup_probes_add.length, 0);
	    }
	});

	testCases.push({
	    'name': 'multi DC, undeployed, configure',
	    'metadata': metadataBasic,
	    'dcConfig': dcconfigs.cfg_multi,
	    'deployed': 'none',
	    'unconfigure': false,
	    'verify': function (plan) {
		assertplus.ok(plan.needsChanges());
		assertplus.strictEqual(plan.mup_groups_remove.length, 0);
		assertplus.strictEqual(plan.mup_probes_remove.length, 0);
		assertplus.strictEqual(plan.mup_groups_add.length, ngroupsfull);
		assertplus.strictEqual(plan.mup_probes_add.length,
		    nprobesmulti);
	    }
	});
}

function runTestCase(tc)
{
	var dc, plan;

	console.error(tc.name);

	dc = tc.dcConfig;
	assertplus.ok(tc.deployed == 'none' || tc.deployed == 'full' ||
	    tc.deployed == 'extra' || tc.deployed == 'partial');
	plan = alarms.amonUpdatePlanCreate({
	    'account': account,
	    'contactsBySeverity': contactsBySeverity,
	    'instances': dc.ctp_instances,
	    'instancesBySvc': dc.ctp_instances_by_svcname,
	    'deployed': tc.deployed == 'none' ?  dc.ctp_deployed_none :
		tc.deployed == 'extra' ? dc.ctp_deployed_extra :
		tc.deployed == 'partial' ? dc.ctp_deployed_partial :
		dc.ctp_deployed_full,
	    'metadata': tc.metadata,
	    'unconfigure': tc.unconfigure
	});
	assertplus.ok(!(plan instanceof Error));
	tc.verify(plan);
}

function makeProbe(params)
{
	var agent, machine;

	agent = params.agent || 'server-uuid-0';
	machine = params.machine || agent;

	return ({
	    'uuid': 'probe-uuid-' + params.name,
	    'name': 'probe-name-' + params.name,
	    'group': params.group || null,
	    'user': account,
	    'type': 'cmd',
	    'config': params.config || {},
	    'agent': agent,
	    'machine': machine,
	    'groupEvents': true
	});
}

function DatacenterConfig()
{
	/* CN uuids for local servers */
	this.ctp_servers = [];
	/* all instances in all DCs (mapping instance uuid -> InstanceInfo) */
	this.ctp_instances = {};
	/* local instances (mapping svcname -> array of instance uuids) */
	this.ctp_instances_by_svcname = {};

	/* destroyed VMs (list of uuids) */
	this.ctp_vms_destroyed = [];

	/* abandoned CNs (list of uuids) */
	this.ctp_servers_abandoned = [];

	/* Deployed Amon objects */
	this.ctp_deployed_full = null;	/* when full */
	this.ctp_deployed_none = null;	/* when empty */
	this.ctp_deployed_extra = null;	/* when full, plus a few other cases */
}

main();
