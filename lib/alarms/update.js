/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/alarms/update.js: facilities for updating a deployed set of Amon probes
 * and probe groups.  This module builds on the facilities provided by
 * config.js.
 */

var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var vasync = require('vasync');
var VError = require('verror');
var extsprintf = require('extsprintf');

var fprintf = extsprintf.fprintf;
var sprintf = extsprintf.sprintf;

var services = require('../services');

var alarm_metadata = require('./metadata');
var alarm_config = require('./config');

/* Exported interface */
exports.amonUpdatePlanCreate = amonUpdatePlanCreate;
exports.amonUpdatePlanSummarize = amonUpdatePlanSummarize;
exports.amonUpdatePlanApply = amonUpdatePlanApply;

/*
 * Amon update plan
 *
 * The MantaAmonUpdatePlan class represents a set of probes and probe groups to
 * be removed and a set of probes and probe groups to be added in order to
 * update the Amon configuration for the Manta service.
 */
function MantaAmonUpdatePlan()
{
	/*
	 * The actual plan is represented by the lists of probes and groups to
	 * be added and removed.
	 */

	this.mup_probes_remove = []; 	/* probes to remove */
	this.mup_groups_remove = []; 	/* probe groups to remove */
	this.mup_groups_add = []; 	/* groups to add */
	this.mup_probes_add = []; 	/* probes to add */

	/*
	 * Statistics kept about the update
	 */

	/* count of probe groups that were deployed and wanted */
	this.mup_ngroupsmatch = 0;
	/* count of probes that were deployed and wanted */
	this.mup_nprobesmatch = 0;
	/* count of probes ignored because they were orphans */
	this.mup_nprobesorphan = 0;
	/* count of probe groups that were deployed, unwanted, but kept */
	this.mup_ngroupsignore = 0;

	/*
	 * Counts of probes added and removed and agents affected, by group id.
	 */

	this.mup_nadd_bygroup = {};
	this.mup_nremove_bygroup = {};
	this.mup_agents_bygroup = {};

	/* warning messages to display to the operator */
	this.mup_warnings = [];

	/*
	 * MantaAmonConfig objects used to generate this plan.
	 */
	this.mup_deployed = null;	/* found configuration */
	this.mup_wanted = null;		/* normal wanted configuration */
	this.mup_unconfigure = false;	/* unconfigure operation */
}

/*
 * This is one of only two methods that may be called from outside of this file.
 * Returns true if the update plan indicates that any changes need to be made.
 */
MantaAmonUpdatePlan.prototype.needsChanges = function ()
{
	return (this.mup_groups_remove.length > 0 ||
	    this.mup_probes_remove.length > 0 ||
	    this.mup_probes_add.length > 0 ||
	    this.mup_groups_add.length > 0);
};

/*
 * This is one of only two methods that may be called from outside of this file.
 * Returns a list of Error objects describing problems found constructing the
 * update plan.  These are generally non-fatal, but should be presented to an
 * operator.
 */
MantaAmonUpdatePlan.prototype.warnings = function ()
{
	return (this.mup_warnings.slice(0));
};

MantaAmonUpdatePlan.prototype.probeUpdate = function (probe, counters, list)
{
	var groupid, agent;

	assertplus.string(probe.p_groupid,
	    'probe has no group id (adding and removing probes ' +
	    'without groups is not supported');
	groupid = probe.p_groupid;
	assertplus.string(probe.p_agent);
	agent = probe.p_agent;

	if (!counters.hasOwnProperty(groupid)) {
		counters[groupid] = 0;
	}
	counters[groupid]++;

	if (!this.mup_agents_bygroup.hasOwnProperty(groupid)) {
		this.mup_agents_bygroup[groupid] = {};
	}
	this.mup_agents_bygroup[groupid][agent] = true;

	list.push(probe);
};

MantaAmonUpdatePlan.prototype.groupAdd = function groupAdd(group)
{
	this.mup_groups_add.push(group);
};

MantaAmonUpdatePlan.prototype.groupRemove = function groupRemove(group)
{
	this.mup_groups_remove.push(group);
};

MantaAmonUpdatePlan.prototype.probeAdd = function probeAdd(probe)
{
	this.probeUpdate(probe, this.mup_nadd_bygroup, this.mup_probes_add);
};

MantaAmonUpdatePlan.prototype.probeRemove = function probeRemove(probe)
{
	this.probeUpdate(probe, this.mup_nremove_bygroup,
	    this.mup_probes_remove);
};

/*
 * Given information about a current deployment, determine the set of updates to
 * Amon necessary to update the configuration to what it should be.  See the
 * block comment in lib/alarms/index.js for a discussion of the goals and
 * constraints of this operation.
 *
 * Named arguments:
 *
 *     account            Triton account uuid to use for wanted Amon probes
 *
 *     contactsBySeverity object mapping event severity levels to the associated
 *                        set of amon contacts
 *
 *     instances          object mapping instance uuids to InstanceInfo objects
 *
 *     instancesBySvc     object mapping SAPI service names to array of instance
 *                        uuids for instances in this datacenter
 *
 *     deployed           MantaAmonConfig object describing the set of probes
 *                        and probe groups curently deployed
 *
 *     metadata           MantaAmonMetadata object describing the set of probes
 *                        and probe groups that should be deployed
 *
 *     unconfigure        if specified, then all probes and probe groups should
 *                        be removed, rather than updated to what would normally
 *                        be configured
 *
 * This function returns either an Error (on failure) or a MantaAmonUpdatePlan.
 */
function amonUpdatePlanCreate(args)
{
	var deployed, metadata, wanted, rv;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.contactsBySeverity, 'args.contactsBySeverity');
	assertplus.object(args.instances, 'args.instances');
	assertplus.object(args.instancesBySvc, 'args.instancesBySvc');
	assertplus.object(args.deployed, 'args.deployed');
	assertplus.ok(args.deployed instanceof alarm_config.MantaAmonConfig);
	assertplus.object(args.metadata, 'args.metadata');
	assertplus.bool(args.unconfigure, 'args.unconfigure');

	deployed = args.deployed;
	metadata = args.metadata;
	wanted = amonGenerateWanted({
	    'account': args.account,
	    'contactsBySeverity': args.contactsBySeverity,
	    'metadata': metadata,
	    'instances': args.instances,
	    'instancesBySvc': args.instancesBySvc
	});

	if (wanted instanceof Error) {
		return (new VError(wanted,
		    'generating wanted amon configuration'));
	}

	rv = new MantaAmonUpdatePlan();
	rv.mup_deployed = deployed;
	rv.mup_wanted = wanted;

	/*
	 * We don't expect to deploy any probes that don't have probe groups
	 * associated with them.
	 */
	wanted.eachOrphanProbe(function (p) {
		throw (new VError(
		    'unexpected orphan probe in "wanted" set'));
	});

	if (args.unconfigure) {
		amonUpdatePlanCreateUnconfigure({
		    'metadata': metadata,
		    'plan': rv
		});

		return (rv);
	}

	/*
	 * Iterate the "wanted" set and create any probe groups and probes that
	 * are missing from the "deployed" set.
	 */
	wanted.eachProbeGroup(function iterWProbeGroup(wpg) {
		var pgname, dpg, probesByAgent;

		pgname = wpg.pg_name;
		dpg = deployed.probeGroupForName(pgname);

		if (dpg !== null) {
			rv.mup_ngroupsmatch++;

			if (!jsprim.deepEqual(wpg.pg_contacts.slice(0).sort(),
			    dpg.pg_contacts.slice(0).sort())) {
				/*
				 * If the contacts on the probe group differ,
				 * then notify the user.  We don't have a way to
				 * update it (see MON-355).  We could remove
				 * everything, but that would make open alarms
				 * harder to grok, so we ask the operator to do
				 * that.
				 */
				rv.mup_warnings.push(new VError('probe group ' +
				    'with name "%s" (deployed with uuid %s): ' +
				    'contacts do not match expected.',
				    pgname, dpg.pg_user));
			}

			if (wpg.pg_user != dpg.pg_user) {
				/*
				 * This is theoretically similar to the
				 * "contacts" case above, but there's no way the
				 * user account should ever change, even with a
				 * reconfiguration.  We'll just ignore that
				 * these are different (but let the operator
				 * know).
				 */
				rv.mup_warnings.push(new VError('probe group ' +
				    'with name "%s" (deployed with uuid %s): ' +
				    'user does not match expected',
				    pgname, dpg.pg_user));
			}
		} else {
			rv.groupAdd(wpg);
		}

		/*
		 * In order to tell which probes need to be added and removed,
		 * we need to be able to match up probes that are deployed with
		 * probes that are wanted.  For our purposes, we will consider
		 * a deployed probe and a wanted probe equivalent if they have
		 * the same value for all of the immutable, configurable fields
		 * that we expect not to change: the probe group name, "type",
		 * "config", "agent", and "machine".  We'll warn if "contacts"
		 * or "groupEvents" don't match what we expect.  If a new
		 * version of the software changes the configuration (e.g., by
		 * changing the bash script executed or the frequency of
		 * execution), the deployed and wanted probes won't match, and
		 * we'll end up removing the deployed one and adding the wanted
		 * one.
		 *
		 * In order to keep this search relatively efficient, we first
		 * build a list of probes for each agent for this probe group.
		 * This should generally correspond to the list of checks
		 * configured in the local metadata.  That's usually just one
		 * probe, but might be a handful.
		 */
		probesByAgent = {};
		if (deployed.hasProbeGroup(pgname)) {
			deployed.eachProbeGroupProbe(pgname,
			    function iterDProbe(p) {
				if (!probesByAgent.hasOwnProperty(p.p_agent)) {
					probesByAgent[p.p_agent] = [];
				}

				probesByAgent[p.p_agent].push(p);
			    });
		}

		wanted.eachProbeGroupProbe(pgname, function iterWProbe(wp) {
			var agent, dprobes, i, dp;

			/*
			 * Try to find a match for this wanted probe in the list
			 * of deployed probes for the same agent.
			 */
			agent = wp.p_agent;
			if (!probesByAgent.hasOwnProperty(agent)) {
				rv.probeAdd(wp);
				return;
			}

			dprobes = probesByAgent[agent];
			for (i = 0; i < dprobes.length; i++) {
				dp = dprobes[i];
				if (dp.p_type == wp.p_type &&
				    jsprim.deepEqual(dp.p_config,
				    wp.p_config) &&
				    dp.p_machine == wp.p_machine) {
					break;
				}
			}

			if (i == dprobes.length) {
				rv.probeAdd(wp);
				return;
			}

			/*
			 * We've found a match, but if it differs in fields we
			 * would never expect to change, warn the administrator.
			 */
			rv.mup_nprobesmatch++;
			if (wp.p_group_events != dp.p_group_events ||
			    (dp.p_contacts === null &&
			    wp.p_contacts !== null) ||
			    (dp.p_contacts !== null &&
			    wp.p_contacts === null) ||
			    (dp.p_contacts !== null &&
			    !jsprim.deepEqual(dp.p_contacts.slice(0).sort(),
			    wp.p_contacts.slice(0).sort()))) {
				rv.mup_warnings.push(new VError('probe group ' +
				    '"%s" (deployed with uuid "%s"): probe ' +
				    'for agent "%s": found match that ' +
				    'differs in "groupEvents" or "contacts"',
				    pgname, dpg.pg_uuid, agent));
			}


			/*
			 * Since we've found a match, there's no action to take
			 * for this probe.  Remove the entry for the deployed
			 * probe so that we can identify all of the deployed
			 * probes that weren't wanted by just iterating what's
			 * left.  This also prevents us from re-using the same
			 * deployed probe to match multiple wanted probes, but
			 * that shouldn't be possible anyway.
			 */
			if (dprobes.length == 1) {
				assertplus.equal(i, 0);
				delete (probesByAgent[agent]);
			} else {
				dprobes.splice(i, 1);
			}
		});

		/*
		 * Remove whatever deployed probes did not match any of the
		 * wanted probes.  We only create each agent's array when we're
		 * going to add to it, and we delete the array entirely when we
		 * would remove its last element, so each array we find here
		 * should be non-empty.
		 */
		jsprim.forEachKey(probesByAgent, function (agent, dprobes) {
			assertplus.ok(dprobes.length > 0);
			dprobes.forEach(function (p) {
				rv.probeRemove(p);
			});
		});
	});

	/*
	 * Now iterate the "deployed" set and remove probes and probe groups
	 * that are both unwanted and eligible for removal.
	 */
	deployed.eachProbeGroup(function iterDProbeGroup(dpg) {
		var pgname;

		pgname = dpg.pg_name;
		if (wanted.probeGroupForName(pgname) !== null) {
			/*
			 * This group was handled when we iterated the wanted
			 * probe groups.
			 */
			return;
		}

		if (!metadata.probeGroupIsRemovable(pgname)) {
			rv.mup_ngroupsignore++;
			return;
		}

		rv.groupRemove(dpg);
		deployed.eachProbeGroupProbe(pgname, function iterDProbe(p) {
			rv.probeRemove(p);
		});
	});

	deployed.eachOrphanProbe(function (p) {
		rv.mup_nprobesorphan++;
	});

	return (rv);
}

/*
 * Given information about deployed VMs and CNs and the local metadata about
 * which probes are to be deployed to which types of components, construct a
 * MantaAmonConfig that represents the desired set of Amon configuration.
 */
function amonGenerateWanted(args)
{
	var contactsBySeverity, wanted, errors, error;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.contactsBySeverity, 'args.contactsBySeverity');
	assertplus.object(args.instances, 'args.instances');
	assertplus.object(args.instancesBySvc, 'args.instancesBySvc');
	assertplus.object(args.metadata, 'args.metadata');

	contactsBySeverity = args.contactsBySeverity;
	wanted = new alarm_config.MantaAmonConfig();
	errors = [];

	args.metadata.eachTemplate(function iterMetadataEvent(pt) {
		var sev;

		sev = pt.pt_ka.ka_severity;
		if (!contactsBySeverity.hasOwnProperty(sev)) {
			/*
			 * Since we construct contactsBySeverity in lib/adm.js,
			 * it's a bug either there or in metadata validation if
			 * we encounter a probe template with an unknown
			 * severity level.
			 */
			throw (new VError(
			    'no contacts defined by caller for alarms with ' +
			    'severity level "%s" (used in %s)', sev,
			    pt.pt_origin_label));
		}

		amonGenerateWantedTemplate({
		    'account': args.account,
		    'contacts': contactsBySeverity[sev].slice(0),
		    'instances': args.instances,
		    'instancesBySvc': args.instancesBySvc,
		    'wanted': wanted,
		    'probeTemplate': pt,
		    'errors': errors
		});
	});

	error = VError.errorFromList(errors);
	return (error !== null ? error : wanted);
}

function amonGenerateWantedTemplate(args)
{
	var events, eventForSvc;
	var instances, instancesBySvc, pt, wanted, errors;

	assertplus.object(args, 'args');
	assertplus.string(args.account, 'args.account');
	assertplus.arrayOfString(args.contacts, 'args.contacts');
	assertplus.object(args.instances, 'args.instances');
	assertplus.object(args.instancesBySvc, 'args.instancesBySvc');
	assertplus.object(args.wanted, 'args.wanted');
	assertplus.ok(args.wanted instanceof alarm_config.MantaAmonConfig);
	assertplus.object(args.probeTemplate, 'args.probeTemplate');
	assertplus.arrayOfObject(args.errors, 'args.errors');

	instances = args.instances;
	instancesBySvc = args.instancesBySvc;
	pt = args.probeTemplate;
	wanted = args.wanted;
	errors = args.errors;

	eventForSvc = {};
	if (pt.pt_scope.ptsc_service == 'each') {
		assertplus.ok(pt.pt_aliases.length > 0);
		events = [];
		pt.pt_aliases.forEach(function (alias) {
			events.push(alias.pta_event);
			eventForSvc[alias.pta_service] =
			    alias.pta_event;
		});
	} else if (pt.pt_scope.ptsc_service == 'all') {
		assertplus.ok(pt.pt_aliases.length === 0);
		events = [ pt.pt_event ];
		jsprim.forEachKey(instancesBySvc, function (svcname) {
			if (services.serviceSupportsProbes(svcname)) {
				eventForSvc[svcname] = pt.pt_event;
			}
		});
	} else if (pt.pt_scope.ptsc_check_from !== null) {
		assertplus.ok(pt.pt_aliases.length === 0);
		events = [ pt.pt_event ];
		eventForSvc[pt.pt_scope.ptsc_check_from] = pt.pt_event;
	} else {
		assertplus.ok(pt.pt_aliases.length === 0);
		events = [ pt.pt_event ];
		eventForSvc[pt.pt_scope.ptsc_service] = pt.pt_event;
	}

	events.forEach(function (eventName) {
		var pgname, error;
		pgname = alarm_metadata.probeGroupNameForTemplate(
		    pt, eventName);

		/*
		 * Undeployed probe groups have no uuid yet.  However, it's
		 * useful for other code to be able to organize data structures
		 * by probe group id.  (They could use probe group names, but
		 * it's not required that probe groups have names, even though
		 * we do require that for our own groups.  Additionally,
		 * sometimes it's useful to organize by probe group uuid even
		 * before a name is known, as when enumerating a bunch of
		 * probes, some of which may even belong to probe groups that no
		 * longer exist, and all we have about them is the uuid.)
		 *
		 * So it's perhaps dicey, but we just fake up a uuid that
		 * matches the name.  We guarantee elsewhere that probe group
		 * names are unique among our own probe groups.
		 */
		error = wanted.addProbeGroup({
		    'uuid': pgname,
		    'name': pgname,
		    'user': args.account,
		    'contacts': args.contacts,
		    'disabled': false
		});

		/*
		 * The only reasons that addProbeGroup can fail are because we
		 * constructed an invalid probe group or one with a duplicate
		 * uuid or name.  That would indicate a bug in this code.
		 */
		if (error !== null) {
			assertplus.ok(error instanceof Error);
			throw (error);
		}
	});

	jsprim.forEachKey(eventForSvc, function (svcname, eventName) {
		var targets, checkers, probeargs, gzs;

		if (!instancesBySvc.hasOwnProperty(svcname)) {
			/*
			 * We have no locally deployed zones for whatever
			 * service we would deploy these probes.  This is likely
			 * to happen if someone is deploying probes in a
			 * partially-deployed Manta, or if this is a
			 * multi-datacenter deployment where some services are
			 * only in a subset of datacenters.  There's nothing
			 * wrong with this; we just have no probes to deploy
			 * here.
			 */
			return;
		}

		checkers = instancesBySvc[svcname];

		if (pt.pt_scope.ptsc_global) {
			/*
			 * If "global" was specified on the scope, then this
			 * probe targets not the zones for the specified
			 * service, but all global zones where this service
			 * runs.  There may be more than one instance on each
			 * CN, so we need to deduplicate this list.
			 */
			gzs = {};
			checkers.forEach(function (c) {
				assertplus.ok(instances.hasOwnProperty(c));
				assertplus.ok(instances[c].inst_local);
				assertplus.string(
				    instances[c].inst_server_uuid);
				gzs[instances[c].inst_server_uuid] = true;
			});
			checkers = Object.keys(gzs);
		}

		if (pt.pt_scope.ptsc_check_from !== null) {
			if (!instancesBySvc.hasOwnProperty(
			    pt.pt_scope.ptsc_check_from)) {
				return;
			}

			targets = instancesBySvc[pt.pt_scope.ptsc_service];
			probeargs = [];
			checkers.forEach(function (c) {
				targets.forEach(function (t) {
					/*
					 * We might expect the machine to be the
					 * "target" here, but amon does not
					 * allow that for probes of type "cmd",
					 * and it's not all that meaningful here
					 * anyway.
					 */
					probeargs.push({
					    'agent': c,
					    'machine': c,
					    'target': t
					});
				});
			});
		} else {
			probeargs = checkers.map(function (c) {
				return ({
				    'agent': c,
				    'machine': c,
				    'target': c
				});
			});
		}

		probeargs.forEach(function (p) {
			pt.pt_checks.forEach(function (check, i) {
				var conf, probe, label, md, error;

				conf = jsprim.deepCopy(check.ptc_config);
				probe = {
				    'name': eventName + i,
				    'type': check.ptc_type,
				    'config': conf,
				    'agent': p.agent,
				    'machine': p.machine,
				    'group': alarm_metadata.
				        probeGroupNameForTemplate(
				        pt, eventName),
				    'groupEvents': true
				};

				/*
				 * Augment probe configurations with information
				 * from SAPI metadata.
				 */
				label = sprintf('probe for group "%s", ' +
				    'check %d, machine "%s"', probe.group,
				    i + 1, p.machine);
				md = instances.hasOwnProperty(p.target) ?
				    instances[p.target].inst_metadata : null;
				amonProbePopulateAutoEnv(label, probe, md,
				    errors);

				/*
				 * As with the call to addProbeGroup() above,
				 * the only reasons this can fail would be
				 * because of bugs in this code.
				 */
				error = wanted.addProbe(probe);
				if (error !== null) {
					assertplus.ok(error instanceof Error);
					throw (error);
				}
			});
		});
	});
}

/*
 * For probes of type "cmd", we support a special configuration property called
 * "autoEnv".  The value of this property is a list of variable names.  We
 * populate the probe's shell environment with corresponding values from the
 * corresponding instance's SAPI metadata.
 */
function amonProbePopulateAutoEnv(label, probe, metadata, errors)
{
	var vars;

	if (probe.type != 'cmd' || !probe.config.hasOwnProperty('autoEnv')) {
		return;
	}

	/*
	 * Remove the autoEnv property itself since Amon doesn't know anything
	 * about that.
	 */
	vars = probe.config.autoEnv;
	delete (probe.config.autoEnv);
	if (vars.length === 0) {
		return;
	}
	if (!probe.config.env) {
		probe.config.env = {};
	}

	if (metadata === null) {
		errors.push(new VError('%s: "autoEnv" specified but no ' +
		    'metadata found for instance', label));
		return;
	}

	vars.forEach(function (v) {
		if (!metadata.hasOwnProperty(v)) {
			errors.push(new VError('%s: "autoEnv" variable "%s": ' +
			    'metadata value not found', label, v));
			return;
		}

		if (typeof (metadata[v]) != 'string') {
			errors.push(new VError('%s: "autoEnv" variable "%s": ' +
			    'metadata value is not a string', label, v));
			return;
		}

		probe.config.env[v] = metadata[v];
	});
}

/*
 * Flesh out an update plan that should unconfigure all of the probes and probe
 * groups that we would normally create.
 *
 * Named arguments:
 *
 *    plan	the update plan to flesh out
 *
 *    metadata	an instanceof MantaAmonMetadata
 */
function amonUpdatePlanCreateUnconfigure(args)
{
	var metadata, plan, wanted, deployed;

	assertplus.object(args, 'args');
	assertplus.object(args.metadata, 'args.metadata');
	assertplus.object(args.plan, 'args.plan');
	assertplus.ok(args.plan instanceof MantaAmonUpdatePlan);

	/*
	 * Unconfiguring isn't quite as simple as it seems.  We want to remove
	 * probe groups that we would normally have configured, as well as probe
	 * groups that we would normally remove (because they were created by
	 * older versions of the software).  But we want to leave in place any
	 * probes and probe groups created by an operator.
	 *
	 * It would be tempting to just create an empty "wanted" configuration
	 * and then run through the usual update plan generation process, but
	 * that process relies on knowing which probe groups are considered
	 * removable (see probeGroupIsRemovable()), and the definition of that
	 * differs for this case because our normal probe groups are removable
	 * when unconfiguring, but not otherwise.
	 */
	metadata = args.metadata;
	plan = args.plan;
	plan.mup_unconfigure = true;
	wanted = plan.mup_wanted;
	deployed = plan.mup_deployed;
	deployed.eachProbeGroup(function iterDProbeGroup(dpg) {
		var pgname, wpg;

		pgname = dpg.pg_name;
		wpg = wanted.probeGroupForName(pgname);
		if (wpg === null &&
		    !metadata.probeGroupIsRemovable(pgname)) {
			plan.mup_ngroupsignore++;
			return (null);
		}

		plan.groupRemove(dpg);
		deployed.eachProbeGroupProbe(pgname, function iterDProbe(p) {
			plan.probeRemove(p);
		});
	});
}

/*
 * Print a human-readable summary of an update plan.  Named arguments:
 *
 *    plan             The update plan to print
 *
 *    stream           Node stream to which to write the summary
 *
 *    instances        object mapping instance uuids to InstanceInfo objects
 *
 *    cns              set of valid CN uuids in this datacenter
 *
 *    vmsDestroyed     set of VMs that are destroyed
 *
 *    cnsAbandoned     set of CNs hosting VMs that are destroyed (and no active
 *                     VMs)
 *
 *    metadata         An instance of MantaAmonMetadata, used to translate
 *                     internal names to more useful titles.
 *
 *    verbose          If true, print detailed information about probes changed
 */
function amonUpdatePlanSummarize(args)
{
	var metadata, out, plan, verbose, instances;
	var nagents, nprobes, ntotagents, ntotprobes, probes;
	var ntotbefore, ntotafter, delta;
	var countsByService = {};

	assertplus.object(args, 'args');
	assertplus.object(args.stream, 'args.stream');
	assertplus.object(args.metadata, 'args.metadata');
	assertplus.object(args.plan, 'args.plan');
	assertplus.ok(args.plan instanceof MantaAmonUpdatePlan);
	assertplus.object(args.instances, 'args.instances');
	assertplus.object(args.cns, 'args.cns');
	assertplus.object(args.vmsDestroyed, 'args.vmsDestroyed');
	assertplus.object(args.cnsAbandoned, 'args.cnsAbandoned');
	assertplus.bool(args.verbose, 'args.verbose');

	metadata = args.metadata;
	out = args.stream;
	plan = args.plan;
	verbose = args.verbose;
	instances = args.instances;

	fprintf(out, 'Probe groups to REMOVE: ');
	if (plan.mup_groups_remove.length === 0) {
		fprintf(out, 'none\n\n');
	} else {
		fprintf(out, '\n\n');
		fprintf(out, '%7s %7s %s\n', 'NPROBES', 'NAGENTS', 'GROUP');
		ntotagents = 0;
		ntotprobes = 0;
		plan.mup_groups_remove.forEach(function (pg) {
			assertplus.ok(plan.mup_nremove_bygroup.hasOwnProperty(
			    pg.pg_uuid));
			assertplus.ok(!plan.mup_nadd_bygroup.hasOwnProperty(
			    pg.pg_uuid));
			nprobes = plan.mup_nremove_bygroup[pg.pg_uuid];

			assertplus.ok(plan.mup_agents_bygroup.hasOwnProperty(
			    pg.pg_uuid));
			nagents = Object.keys(plan.mup_agents_bygroup[
			    pg.pg_uuid]).length;

			fprintf(out, '%7d %7d %s\n',
			    nprobes, nagents, pg.pg_name);

			ntotprobes += nprobes;
			ntotagents += nagents;
		});
		fprintf(out, '%7d %7d TOTAL\n\n', ntotprobes, ntotagents);
	}

	fprintf(out, 'Probe groups to ADD: ');
	if (plan.mup_groups_add.length === 0) {
		fprintf(out, 'none\n\n');
	} else {
		fprintf(out, '\n\n');
		fprintf(out, '%7s %7s %s\n', 'NPROBES', 'NAGENTS', 'GROUP');
		ntotagents = 0;
		ntotprobes = 0;
		plan.mup_groups_add.forEach(function (pg) {
			var evt, ka, name;

			assertplus.ok(!plan.mup_nremove_bygroup.hasOwnProperty(
			    pg.pg_uuid));

			/*
			 * It's possible that we would create a probe group that
			 * has no probes.  This likely means there are no
			 * instances of the zone that this group is associated
			 * with.  This would usually happen in a multi-DC
			 * deployment where there happen to be no instances in
			 * this datacenter.
			 */
			if (plan.mup_nadd_bygroup.hasOwnProperty(pg.pg_uuid)) {
				nprobes = plan.mup_nadd_bygroup[pg.pg_uuid];
				assertplus.ok(plan.mup_agents_bygroup.
				    hasOwnProperty(pg.pg_uuid));
				nagents = Object.keys(plan.mup_agents_bygroup[
				    pg.pg_uuid]).length;
			} else {
				assertplus.ok(!plan.mup_agents_bygroup.
				    hasOwnProperty(pg.pg_uuid));
				nprobes = 0;
				nagents = 0;
			}

			name = pg.pg_name;
			evt = metadata.probeGroupEventName(pg.pg_name);
			if (evt !== null) {
				ka = metadata.eventKa(evt);
				if (ka !== null) {
					name = ka.ka_title;
				}
			}

			fprintf(out, '%7d %7d %s\n', nprobes, nagents, name);

			ntotprobes += nprobes;
			ntotagents += nagents;
		});
		fprintf(out, '%7d %7d TOTAL\n\n', ntotprobes, ntotagents);
	}

	fprintf(out, 'Count of probes by service:\n\n');
	fprintf(out, '    %-16s  %6s  %6s  %6s\n', 'SERVICE', 'BEFORE', 'AFTER',
	    'DELTA');
	services.mSvcNamesProbes.forEach(function (svcname) {
		countsByService[svcname] = {
		    'sc_before': 0,
		    'sc_after': 0
		};
	});
	countsByService['global zones'] = {
	    'sc_before': 0,
	    'sc_after': 0
	};
	countsByService['destroyed VMs'] = {
	    'sc_before': 0,
	    'sc_after': 0
	};
	countsByService['abandoned CNs'] = {
	    'sc_before': 0,
	    'sc_after': 0
	};

	ntotbefore = amonUpdatePlanSummarizeConfig({
	    'config': plan.mup_deployed,
	    'cns': args.cns,
	    'countsByService': countsByService,
	    'instances': instances,
	    'out': out,
	    'propname': 'sc_before',
	    'vmsDestroyed': args.vmsDestroyed,
	    'cnsAbandoned': args.cnsAbandoned
	});

	ntotafter = 0;
	if (!plan.mup_unconfigure) {
		ntotafter = amonUpdatePlanSummarizeConfig({
		    'config': plan.mup_wanted,
		    'cns': args.cns,
		    'countsByService': countsByService,
		    'instances': instances,
		    'out': out,
		    'propname': 'sc_after',
		    'vmsDestroyed': args.vmsDestroyed,
		    'cnsAbandoned': args.cnsAbandoned
		});
	}

	jsprim.forEachKey(countsByService, function (svcname, counts) {
		delta = counts.sc_after - counts.sc_before;
		fprintf(out, '    %-16s  %6d  %6d  %6s\n', svcname,
		    counts.sc_before, counts.sc_after,
		    delta > 0 ? '+' + delta : delta);
	});
	delta = ntotafter - ntotbefore;
	fprintf(out, '    %-16s  %6d  %6d  %6s\n', 'TOTAL',
	    ntotbefore, ntotafter,
	    delta > 0 ? '+' + delta : delta);
	fprintf(out, '\n');

	if (verbose) {
		fprintf(out, 'Probes to ADD:\n');
		probes = plan.mup_probes_add.slice(0).sort(function (p1, p2) {
			var s1, s2, rv;

			s1 = instances[p1.p_agent].inst_svcname;
			s2 = instances[p2.p_agent].inst_svcname;
			rv = s1.localeCompare(s2);
			if (rv !== 0) {
				return (rv);
			}

			rv = p1.p_agent.localeCompare(p2.p_agent);
			if (rv !== 0) {
				return (rv);
			}

			/*
			 * We do not allow our own probes to be nameless.
			 */
			assertplus.string(p1.p_name);
			assertplus.string(p2.p_name);
			return (p1.p_name.localeCompare(p2.p_name));
		});

		probes.forEach(function (p) {
			fprintf(out, '    %s %-16s %s\n', p.p_agent,
			    instances[p.p_agent].inst_svcname, p.p_name);
		});
		fprintf(out, '\n');
	}

	fprintf(out, 'Summary:\n\n');
	fprintf(out, '%6d wanted probe groups matched existing groups\n',
	    plan.mup_ngroupsmatch);
	fprintf(out, '%6d wanted probes matched existing probes\n',
	    plan.mup_nprobesmatch);
	fprintf(out, '%6d probes ignored because they were in no probe group\n',
	    plan.mup_nprobesorphan);
	fprintf(out, '%6d probe groups ignored (operator-added)\n',
	    plan.mup_ngroupsignore);
	fprintf(out, '%6d total probe groups to remove\n',
	    plan.mup_groups_remove.length);
	fprintf(out, '%6d total probes to remove\n',
	    plan.mup_probes_remove.length);
	fprintf(out, '%6d total probe groups to add\n',
	    plan.mup_groups_add.length);
	fprintf(out, '%6d total probes to add\n', plan.mup_probes_add.length);
	fprintf(out, '%6d warnings\n\n', plan.mup_warnings.length);

	plan.mup_warnings.forEach(function (w) {
		fprintf(out, 'warn: %s\n', w.message);
	});
}

function amonUpdatePlanSummarizeConfig(args)
{
	var config, countsByService, cns, instances, out, propname;
	var vmsDestroyed, cnsAbandoned;
	var total = 0;

	assertplus.object(args.config, 'args.config');
	assertplus.object(args.cns, 'args.cns');
	assertplus.object(args.instances, 'args.instances');
	assertplus.object(args.countsByService, 'args.countsByService');
	assertplus.object(args.vmsDestroyed, 'args.vmsDestroyed');
	assertplus.object(args.cnsAbandoned, 'args.cnsAbandoned');
	assertplus.object(args.out, 'args.out');
	assertplus.string(args.propname, 'args.propname');

	config = args.config;
	countsByService = args.countsByService;
	cns = args.cns;
	cnsAbandoned = args.cnsAbandoned;
	instances = args.instances;
	vmsDestroyed = args.vmsDestroyed;
	out = args.out;
	propname = args.propname;

	config.eachProbeGroup(function (pg) {
		config.eachProbeGroupProbe(pg.pg_name, function (p) {
			var agent, svcname;

			assertplus.string(p.p_agent);
			agent = p.p_agent;
			if (instances.hasOwnProperty(agent)) {
				svcname = instances[agent].inst_svcname;
			} else if (cns.hasOwnProperty(agent)) {
				svcname = 'global zones';
			} else if (vmsDestroyed.hasOwnProperty(agent)) {
				svcname = 'destroyed VMs';
			} else if (cnsAbandoned.hasOwnProperty(agent)) {
				svcname = 'abandoned CNs';
			} else {
				fprintf(out, 'warning: probe "%s": agent ' +
				    '"%s" is not a known VM or CN\n',
				    p.p_uuid, p.p_agent);
				return;
			}

			assertplus.ok(countsByService.hasOwnProperty(svcname));
			countsByService[svcname][propname]++;
			total++;
		});
	});

	return (total);
}

/*
 * Apply the changes described by a MantaUpdatePlan.  This removes old probes
 * and probe groups and creates new ones to replace them.  This operation is not
 * atomic, and can wind up in basically any intermediate state.  However, the
 * broader operation (where we construct the update plan and then apply it) is
 * idempotent.  In the face of only transient errors, this process can be
 * re-applied to converge to the desired state.
 */
function amonUpdatePlanApply(args, callback)
{
	var plan, out, au;

	assertplus.object(args, 'args');
	assertplus.object(args.plan, 'args.plan');
	assertplus.object(args.amon, 'args.amon');
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.string(args.account, 'args.account');
	assertplus.object(args.stream, 'args.stream');

	plan = args.plan;
	out = args.stream;
	au = new AmonUpdate(args);

	/*
	 * We always create probes inside probe groups.  In order to represent
	 * probes before we've created those probe groups, the "p_groupid"
	 * property for new probes identifies the name (not uuid) of the group
	 * they will be in.  (We assume that group names are unique, and this is
	 * validated elsewhere.)  When we create these probes shortly, we'll
	 * need to look up the real uuid of the group.  There are two cases:
	 * either the probe group already exists, in which case we have its uuid
	 * right now, or the probe group will be created by this process, in
	 * which case we'll need to record that and use it later.
	 *
	 * Here, we collect the names and uuids of probe groups that already
	 * exist and add them to mau_groups_byname.  As we create new probe
	 * groups, we'll add their names and uuids to the same structure.  We'll
	 * consult this when we go create new probes.
	 */
	jsprim.forEachKey(au.mau_plan.mup_deployed.mac_probegroups_by_name,
	    function forEachDeployedProbeGroup(name, group) {
		au.mau_groups_byname[name] = group.pg_uuid;
	    });

	/*
	 * Although Amon may tolerate probes whose groups are missing, we avoid
	 * creating such a state by processing each of these phases separately.
	 * Strictly speaking, we only need three phases to do this: remove old
	 * probes, remove and create probe groups, and create new probes.  It's
	 * simpler (and not much slower) to split this middle phase.
	 */
	fprintf(out, 'Applying changes ... ');
	vasync.pipeline({
	    'input': null,
	    'funcs': [
		function amonUpdateRemoveProbes(_, subcallback) {
			amonUpdateQueue(au, plan.mup_probes_remove,
			    amonUpdateProbeRemove, subcallback);
		},
		function amonUpdateRemoveProbeGroups(_, subcallback) {
			amonUpdateQueue(au, plan.mup_groups_remove,
			    amonUpdateGroupRemove, subcallback);
		},
		function amonUpdateAddProbeGroups(_, subcallback) {
			amonUpdateQueue(au, plan.mup_groups_add,
			    amonUpdateGroupAdd, subcallback);
		},
		function amonUpdateAddProbes(_, subcallback) {
			amonUpdateQueue(au, plan.mup_probes_add,
			    amonUpdateProbeAdd, subcallback);
		}
	    ]
	}, function (err) {
		fprintf(out, 'done.\n');
		fprintf(out, 'probes removed: %5d\n', au.mau_nprobes_removed);
		fprintf(out, 'groups removed: %5d\n', au.mau_ngroups_removed);
		fprintf(out, 'groups added:   %5d\n', au.mau_ngroups_added);
		fprintf(out, 'probes added:   %5d\n', au.mau_nprobes_added);
		callback(err);
	});
}

/*
 * Represents the state associated with a single amon update operation.
 * This class is used as a struct, with details private to this subsystem.
 */
function AmonUpdate(args)
{
	assertplus.object(args, 'args');
	assertplus.object(args.amon, 'args.amon');
	assertplus.object(args.plan, 'args.plan');
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.string(args.account, 'args.account');

	this.mau_amon = args.amon;
	this.mau_concurrency = args.concurrency;
	this.mau_plan = args.plan;
	this.mau_account = args.account;
	this.mau_queues = [];
	this.mau_errors = [];
	this.mau_groups_byname = {};

	/* for debugging */
	this.mau_nprobes_removed = 0;
	this.mau_ngroups_removed = 0;
	this.mau_ngroups_added = 0;
	this.mau_nprobes_added = 0;
}

/*
 * Given a worker function, pushes all of the specified inputs through a queue.
 */
function amonUpdateQueue(au, tasks, worker, callback)
{
	var queue;

	queue = vasync.queuev({
	    'concurrency': au.mau_concurrency,
	    'worker': function queueWorker(task, qcallback) {
		worker(au, task, function onWorkDone(err) {
			if (err) {
				au.mau_errors.push(err);
			}

			qcallback();
		});
	    }
	});

	au.mau_queues.push(queue);

	tasks.forEach(function (t) {
		queue.push(t);
	});

	queue.on('end', function () {
		callback(VError.errorFromList(au.mau_errors));
	});
	queue.close();
}

function amonUpdateProbeAdd(au, probe, callback)
{
	var newprobe;

	/*
	 * We must not have assigned a uuid by this point, but we only create
	 * probes that have names.
	 */
	assertplus.strictEqual(probe.p_uuid, null);
	assertplus.notStrictEqual(probe.p_name, null);
	newprobe = {
	    'name': probe.p_name,
	    'type': probe.p_type,
	    'config': probe.p_config,
	    'agent': probe.p_agent,
	    'machine': probe.p_machine || undefined,
	    'contacts': probe.p_contacts,
	    'groupEvents': probe.p_group_events
	};

	/*
	 * By this point in the process, we must have a name -> uuid mapping for
	 * the group associated with this probe.
	 */
	assertplus.ok(au.mau_groups_byname.hasOwnProperty(probe.p_groupid));
	newprobe.group = au.mau_groups_byname[probe.p_groupid];

	au.mau_amon.createProbe(au.mau_account, newprobe,
	    function onAmonProbeAdd(err) {
		if (err) {
			err = new VError(err, 'add probe "%s"', probe.p_name);
		} else {
			au.mau_nprobes_added++;
		}

		callback(err);
	    });
}

function amonUpdateProbeRemove(au, probe, callback)
{
	assertplus.string(probe.p_uuid);
	au.mau_amon.deleteProbe(au.mau_account, probe.p_uuid,
	    function onAmonProbeRemove(err) {
		if (err) {
			err = new VError(err, 'remove probe "%s"',
			    probe.p_uuid);
		} else {
			au.mau_nprobes_removed++;
		}

		callback(err);
	    });
}

function amonUpdateGroupAdd(au, group, callback)
{
	var newgroup;

	/*
	 * Prior to this point, the uuid matches the name.
	 */
	assertplus.strictEqual(group.pg_uuid, group.pg_name);
	newgroup = {
	    'name': group.pg_name,
	    'user': group.pg_user,
	    'contacts': group.pg_contacts
	};

	assertplus.ok(!au.mau_groups_byname.hasOwnProperty(group.pg_name));
	au.mau_amon.createProbeGroup(au.mau_account, newgroup,
	    function onAmonGroupAdd(err, amongroup) {
		if (!err && typeof (amongroup.uuid) != 'string') {
			err = new VError('amon returned group with bad or ' +
			    'missing uuid');
		}

		if (!err && amongroup.name != group.pg_name) {
			err = new VError('amon returned group with a ' +
			    'different name (uuid "%s")', amongroup.uuid);
		}

		if (err) {
			err = new VError(err, 'add group "%s"', group.pg_uuid);
			callback(err);
			return;
		}

		assertplus.ok(!au.mau_groups_byname.hasOwnProperty(
		    group.pg_name));
		au.mau_groups_byname[group.pg_name] = amongroup.uuid;
		au.mau_ngroups_added++;
		callback(err);
	    });
}

function amonUpdateGroupRemove(au, group, callback)
{
	assertplus.string(group.pg_uuid);
	au.mau_amon.deleteProbeGroup(au.mau_account, group.pg_uuid,
	    function onAmonGroupRemove(err) {
		if (err) {
			err = new VError(err, 'remove group "%s"',
			    group.pg_uuid);
		} else {
			au.mau_ngroups_removed++;
		}

		callback(err);
	    });
}
