/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/alarms/config.js: facilities for representing a set of amon
 * configuration, which essentially means a set of probes and probe groups.
 */

var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var progbar = require('progbar');
var vasync = require('vasync');
var VError = require('verror');
var fprintf = require('extsprintf').fprintf;

var amon_objects = require('./amon_objects');
var services = require('../services');

/* Exported interface */
exports.amonLoadProbeGroups = amonLoadProbeGroups;
exports.amonLoadComponentProbes = amonLoadComponentProbes;
exports.amonConfigSummarize = amonConfigSummarize;
exports.MantaAmonConfig = MantaAmonConfig;

/*
 * Fetches Amon probe groups.
 *
 *     amon             an Amon client
 *
 *     account		Triton account uuid whose probes to fetch
 *
 * callback is invoked as "callback(err, amonconfig)", where on success
 * "amonconfig" is an instance of MantaAmonConfig.  Note that "err" and
 * "amonconfig" can both be non-null, in which case "err" represents non-fatal
 * (warning-level) issues encountered.
 */
function amonLoadProbeGroups(args, callback)
{
	var account, amon;

	assertplus.object(args, 'args');
	assertplus.object(args.amon, 'args.amon');
	assertplus.func(callback, 'callback');

	account = args.account;
	amon = args.amon;
	amon.listProbeGroups(account, function (err, rawgroups) {
		var amoncfg, errors;

		if (err) {
			err = new VError(err, 'listing probegroups');
			callback(err);
			return;
		}

		amoncfg = new MantaAmonConfig();
		errors = [];
		rawgroups.forEach(function (rawgroup) {
			var error = amoncfg.addProbeGroup(rawgroup);
			if (error instanceof Error) {
				errors.push(
				    new VError(error, 'ignoring group'));
			}
		});

		err = VError.errorFromList(errors);
		callback(err, amoncfg);
	});
}

/*
 * Fetches Amon probe objects for all probes for the specified components.
 * Named arguments:
 *
 *     amonRaw          a restify JSON client for the AMON master API.
 *     			This is different from most other consumers, which use
 *     			an actual Amon client.
 *
 *     amoncfg          an instance of MantaAmonConfig with probe groups
 *                      configured already.  This configuration will be updated
 *                      with probe details.
 *
 *     components	an array of objects describing the components.  Each
 *     			component should have properties:
 *
 *     		"type"	either "cn" (for compute nodes) or "vm" (for containers)
 *
 *     		"uuid"  the server_uuid (for type "cn") or VM uuid (for
 *     			containers)
 *
 *     concurrency	an integer number for the maximum concurrent requests
 *
 * "callback" is invoked as "callback(err)".
 *
 * Amon has an API for listing probes, but it's limited to 1000 probes, which is
 * too small for large Manta deployments.  Additionally, that API has no support
 * for pagination.  Instead, we use the private Amon agent API to fetch the list
 * of probes for each agent.  That number is generally much smaller.  This
 * results in a lot more requests, but we don't have a better option.
 */
function amonLoadComponentProbes(args, callback)
{
	var amoncfg, client, queue, errors, warnings, progress, ndone;

	assertplus.object(args, 'args');
	assertplus.object(args.amonRaw, 'args.amonRaw');
	assertplus.object(args.amoncfg, 'args.amoncfg');
	assertplus.ok(args.amoncfg instanceof MantaAmonConfig);
	assertplus.number(args.concurrency, 'args.concurrency');
	assertplus.arrayOfObject(args.components, 'args.components');
	assertplus.func(callback, 'callback');

	amoncfg = args.amoncfg;
	client = args.amonRaw;
	errors = [];
	warnings = [];
	ndone = 0;
	if (process.stderr.isTTY) {
		progress = new progbar.ProgressBar({
		    'filename': 'fetching probes for each agent',
		    'bytes': false,
		    'size': args.components.length
		});
	}

	queue = vasync.queuev({
	    'concurrency': args.concurrency,
	    'worker': function fetchProbeQueueWorker(component, qcallback) {
		assertplus.object(component, 'component');
		assertplus.string(component.type, 'component.type');
		assertplus.string(component.uuid, 'component.uuid');

		amonFetchAgentProbes({
		    'amon': client,
		    'agentUuid': component.uuid
		}, function (err, probes) {
			if (err) {
				err = new VError(err, 'fetching probes for ' +
				    'agent on %s "%s"', component.type,
				    component.uuid);
				errors.push(err);
				qcallback();
				return;
			}

			probes.forEach(function (p) {
				var error = amoncfg.addProbe(p);
				if (error !== null) {
					warnings.push(new VError(error,
					    'ignoring probe'));
				}
			});

			ndone++;
			if (progress !== undefined) {
				progress.advance(ndone);
			}

			qcallback();
		});
	    }
	});

	args.components.forEach(function (c, i) {
		var label = 'args.components[' + i + ']';
		assertplus.string(c.type, label + '.type');
		assertplus.string(c.uuid, label + '.uuid');
		queue.push({ 'type': c.type, 'uuid': c.uuid });
	});

	queue.on('end', function () {
		if (progress !== undefined) {
			progress.end();
		}

		callback(VError.errorFromList(errors),
		    VError.errorFromList(warnings));
	});

	queue.close();
}

/*
 * Uses the amon (private) relay API to list the probes associated with the
 * given agent.
 *
 * Named arguments:
 *
 *     amon             a restify JSON client for the AMON master API
 *
 *     agentUuid        uuid of the agent whose probes should be fetched
 */
function amonFetchAgentProbes(args, callback)
{
	var client, uripath;

	assertplus.object(args, 'args');
	assertplus.object(args.amon, 'args.amon');
	assertplus.string(args.agentUuid, 'args.agentUuid');
	assertplus.func(callback, 'callback');

	client = args.amon;
	uripath = '/agentprobes?agent=' + encodeURIComponent(args.agentUuid);
	client.get(uripath, function (err, req, res, result) {
		/*
		 * This API has the same problem as most Amon API "list"
		 * operations, which is that they implicitly have a limit on the
		 * number of results, there's no way to override that, and
		 * there's no way to paginate the list.  As a result, we can
		 * never see more than that many results.  Today, this number is
		 * 1000.  We attempt to at least detect that this might have
		 * happened.
		 */
		var limit = 1000;
		if (!err && result.length == limit) {
			err = new VError('got %d results, ' +
			    'assuming truncation', limit);
		}

		if (err) {
			err = new VError(err, 'amon: get "%s"', uripath);
			callback(err);
			return;
		}

		callback(null, result);
	});
}


/*
 * Print a human-readable summary of configured probes and probe groups.
 */
function amonConfigSummarize(args, callback)
{
	var out, config, metadata, instanceSvcname;
	var ngroups, nagents, nprobes, norphans;
	var svcs, rows;

	assertplus.object(args, 'args');
	assertplus.object(args.config, 'args.config');
	assertplus.ok(args.config instanceof MantaAmonConfig);
	assertplus.object(args.stream, 'args.stream');
	assertplus.object(args.metadata, 'args.metadata');
	assertplus.object(args.instanceSvcname, 'args.instanceSvcname');

	out = args.stream;
	config = args.config;
	metadata = args.metadata;
	instanceSvcname = args.instanceSvcname;

	ngroups = 0;
	nprobes = 0;
	nagents = 0;
	norphans = 0;
	svcs = {};
	svcs['unknown'] = {
	    'svc_groups': {},
	    'svc_nprobes': 0,
	    'svc_ninstances': 0,
	    'svc_norphans': 0,
	    'svc_agents': {}
	};
	svcs['global zone'] = jsprim.deepCopy(svcs['unknown']);
	services.mSvcNamesProbes.forEach(function (svcname) {
		svcs[svcname] = jsprim.deepCopy(svcs['unknown']);
	});

	/*
	 * Print a count of probes and agents affected for each probe group.
	 */
	rows = [];
	config.eachProbeGroup(function (pg) {
		var eventName, ka, name;
		var ngroupprobes, agents, ngroupagents;

		agents = {};
		ngroupprobes = 0;
		config.eachProbeGroupProbe(pg.pg_name, function iterProbe(p) {
			var svcname, svc;

			agents[p.p_agent] = true;
			ngroupprobes++;
			svcname = instanceSvcname.hasOwnProperty(p.p_agent) &&
			    svcs.hasOwnProperty(instanceSvcname[p.p_agent]) ?
			    instanceSvcname[p.p_agent] : 'unknown';
			svc = svcs[svcname];
			svc.svc_nprobes++;
			svc.svc_groups[pg.pg_uuid] = true;
			svc.svc_agents[p.p_agent] = true;
		});

		ngroupagents = Object.keys(agents).length;
		nprobes += ngroupprobes;
		ngroups++;

		eventName = metadata.probeGroupEventName(pg.pg_name);
		if (eventName !== null) {
			ka = metadata.eventKa(eventName);
			if (ka !== null) {
				name = ka.ka_title;
			} else {
				name = eventName;
			}
		} else {
			name = pg.pg_name;
		}

		rows.push({
		    'nprobes': ngroupprobes,
		    'nagents': ngroupagents,
		    'name': name
		});
	});

	config.eachOrphanProbe(function (p) {
		var svcname, svc;

		nprobes++;
		norphans++;
		svcname = instanceSvcname.hasOwnProperty(p.p_agent) &&
		    svcs.hasOwnProperty(instanceSvcname[p.p_agent]) ?
		    instanceSvcname[p.p_agent] : 'unknown';
		svc = svcs[svcname];
		svc.svc_nprobes++;
		svc.svc_norphans++;
	});

	fprintf(out, 'Configuration by probe group:\n\n');
	fprintf(out, '    %7s  %7s  %s\n', 'NPROBES', 'NAGENTS', 'PROBE GROUP');
	rows.sort(function (r1, r2) {
		return (r1.name.localeCompare(r2.name));
	}).forEach(function (row) {
		fprintf(out, '    %7d  %7d  %s\n',
		    row.nprobes, row.nagents, row.name);
	});

	/*
	 * Now print a summary of probes by service name.
	 */
	fprintf(out, '\nConfiguration by service:\n\n');
	fprintf(out, '    %-16s  %7s  %7s  %7s  %8s\n',
	    'SERVICE', 'NGROUPS', 'NAGENTS', 'NPROBES', 'NORPHANS');
	Object.keys(svcs).sort(function (a1, a2) {
		if (a1 == 'unknown') {
			return (1);
		} else if (a2 == 'unknown') {
			return (-1);
		}
		return (a1.localeCompare(a2));
	}).forEach(function (svcname) {
		var svc, nsvcagents;

		svc = svcs[svcname];
		nsvcagents = Object.keys(svc.svc_agents).length;
		fprintf(out, '    %-16s  %7d  %7d  %7d  %8d\n', svcname,
		    Object.keys(svc.svc_groups).length, nsvcagents,
		    svc.svc_nprobes, svc.svc_norphans);
		nagents += nsvcagents;
	});

	fprintf(out, '    %-16s  %7d  %7d  %7d  %8d\n\n', 'TOTAL',
	    ngroups, nagents, nprobes, norphans);
}


/*
 * Amon configuration
 *
 * The MantaAmonConfig class represents a set of probes and probe groups.  See
 * the block comment in lib/alarms/index.js for more information.
 *
 * This implementation requires that probe group names be unique, and that probe
 * groups be added before probes.  The name uniqueness constraint is important
 * because the only way to compare what we expect to be deployed against what's
 * really deployed is based on the probe group names.  If we have more than one
 * probe group with the same name, then it would be much harder to tell whether
 * the right probes were deployed.
 */
function MantaAmonConfig()
{
	/*
	 * mapping of probe group name -> probe group object
	 * This is the canonical set of probe groups represented by this object.
	 */
	this.mac_probegroups_by_name = {};

	/*
	 * mapping of probe group uuid -> probe group name
	 * This set is updated as callers add probe groups, but it's primarily
	 * used as callers subsequently add probes in order to map those probes
	 * to corresponding probe groups.  It's also used for deployed probe
	 * groups to allow consumers to map group uuids to group names.
	 */
	this.mac_probegroups_by_uuid = {};

	/*
	 * mapping of probe group name -> list of probes
	 * Along with mac_probes_orphan below, this is the canonical set of
	 * probes represented by this object.
	 */
	this.mac_probes_by_probegroup = {};

	/* List of probes having no group */
	this.mac_probes_orphan = [];
}

/*
 * Adds a probe.  The "probedef" object must match the Amon schema for a probe.
 */
MantaAmonConfig.prototype.addProbe = function (probedef)
{
	var probe, pgname;

	probe = new amon_objects.loadProbeObject(probedef);
	if (probe instanceof Error) {
		return (probe);
	}

	if (probe.p_groupid === null) {
		this.mac_probes_orphan.push(probe);
		return (null);
	}

	if (!this.mac_probegroups_by_uuid.hasOwnProperty(probe.p_groupid)) {
		return (new VError('probe "%s": unknown probe group "%s"',
		    probe.p_uuid, probe.p_groupid));
	}

	pgname = this.mac_probegroups_by_uuid[probe.p_groupid];
	assertplus.ok(this.mac_probes_by_probegroup.hasOwnProperty(pgname));
	this.mac_probes_by_probegroup[pgname].push(probe);
	return (null);
};

/*
 * Adds a probe group.  The "groupdef" object must match the Amon schema for a
 * probe group.
 */
MantaAmonConfig.prototype.addProbeGroup = function (groupdef)
{
	var probegroup;

	probegroup = amon_objects.loadProbeGroupObject(groupdef);
	if (probegroup instanceof Error) {
		return (probegroup);
	}

	if (this.mac_probegroups_by_name.hasOwnProperty(probegroup.pg_name)) {
		return (new VError('duplicate probe group name: "%s"',
		    probegroup.pg_name));
	}

	if (this.mac_probegroups_by_uuid.hasOwnProperty(probegroup.pg_uuid)) {
		return (new VError('duplicate probe group uuid: "%s"',
		    probegroup.pg_uuid));
	}

	assertplus.ok(!this.mac_probes_by_probegroup.hasOwnProperty(
	    probegroup.pg_name));
	this.mac_probegroups_by_name[probegroup.pg_name] = probegroup;
	this.mac_probegroups_by_uuid[probegroup.pg_uuid] = probegroup.pg_name;
	this.mac_probes_by_probegroup[probegroup.pg_name] = [];
	return (null);
};

/*
 * Returns the specified probe group, if it exists.  Otherwise, returns null.
 */
MantaAmonConfig.prototype.probeGroupForName = function (pgname)
{
	assertplus.string(pgname, 'pgname');
	if (!this.mac_probegroups_by_name.hasOwnProperty(pgname)) {
		return (null);
	}

	return (this.mac_probegroups_by_name[pgname]);
};

/*
 * Returns the probe group name for the given probe group id.
 */
MantaAmonConfig.prototype.probeGroupNameForUuid = function (pgid)
{
	if (!this.mac_probegroups_by_uuid.hasOwnProperty(pgid)) {
		return (null);
	}

	return (this.mac_probegroups_by_uuid[pgid]);
};

MantaAmonConfig.prototype.hasProbeGroup = function (pgname)
{
	assertplus.string(pgname);
	return (this.mac_probes_by_probegroup.hasOwnProperty(pgname));
};

/*
 * Iterates all of the probe groups in this configuration and invokes
 * "func(probegroup)".
 */
MantaAmonConfig.prototype.eachProbeGroup = function (func)
{
	var probesbypg;

	assertplus.func(func, 'func');
	probesbypg = this.mac_probes_by_probegroup;
	jsprim.forEachKey(this.mac_probegroups_by_name, function (name, pg) {
		assertplus.ok(probesbypg.hasOwnProperty(name));
		func(pg);
	});
};

/*
 * Iterates all probes in this configuration that are associated with probe
 * group "pgname" and invokes "func(probe)" for each one.
 */
MantaAmonConfig.prototype.eachProbeGroupProbe = function (pgname, func)
{
	var probes;
	assertplus.string(pgname, 'pgname');
	assertplus.func(func, 'func');
	assertplus.ok(this.mac_probes_by_probegroup.hasOwnProperty(pgname),
	    'unknown probe group name: "' + pgname + '"');
	probes = this.mac_probes_by_probegroup[pgname];
	probes.forEach(function (p) { func(p); });
};

/*
 * Iterates all probes in this configuration that have no associated probe
 * group and invokes "func(probe)" for each one.
 */
MantaAmonConfig.prototype.eachOrphanProbe = function (func)
{
	assertplus.func(func, 'func');
	this.mac_probes_orphan.forEach(function (p) { func(p); });
};
