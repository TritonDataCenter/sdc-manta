/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/alarms/metadata.js: facilities for working with locally-provided metadata
 * about probes and probe groups.  See the block comment in lib/alarms/index.js
 * for details.
 *
 *
 * INTERFACES
 *
 * This module exposes the following function publicly:
 *
 *     loadMetadata: loads locally-provided metadata from files into a
 *     MantaAmonMetadata object
 *
 * That function implicitly exposes this class:
 *
 *     MantaAmonMetadata: a class that provides basic methods for iterating the
 *     locally-provided metadata.  Instances of this class are immutable once
 *     constructed.
 *
 * This module exposes the following function semi-privately (to other modules
 * in this directory):
 *
 *     probeGroupNameForTemplate: constructs a probe group name based on a probe
 *     template
 *
 * as well as the "MetadataLoader" for tools.
 *
 *
 * PROBE TEMPLATE FILES
 *
 * In this repo, the directory "alarm_metadata/probe_templates" contains a
 * number of probe template files written in YAML.  Each file describes an array
 * of probe templates.  The probe templates from all of these files are combined
 * into a single configuration; the organization into separate files is purely
 * for readers.  Each probe template describes a distinct failure mode for a
 * Manta deployment and implicitly specifies a group of probes to be created at
 * deployment-time.  These concepts and the broad design are described in much
 * more detail in lib/alarms/index.js.
 *
 * We use YAML because of its reasonable support of strings with embedded
 * newlines.  These probe template files contain both ordinary configuration
 * (that could be specified in JSON as well as anything else) and a bunch of
 * human-readable strings (each containing potentially a few paragraphs) that
 * are closely associated with that configuration.  These files are not used by
 * tools outside this repository, so the format can be changed in the future.
 *
 * Each probe template MUST contain these top-level properties:
 *
 *    "event"                   a unique, FMA-style event class name.  This
 *    (required string)         ultimately defines a probe group.  All probes
 *                              created from this template will be part of the
 *                              same probe group (and there will be no other
 *                              probes in this probe group).
 *
 *    "scope"                   describes the set of components that this
 *    (required object)         probe template is intended to monitor, as well
 *                              as how to distribute probes for those components
 *
 *        "scope.service"       identifies the SAPI service being monitored.
 *        (required string)     In most cases, this template will generate an
 *                              Amon probe for each instance of the specified
 *                              service.
 *
 *        "scope.global"        if true, then instead of creating a probe for
 *        (optional boolean)    each instance of the SAPI service indicated by
 *                              "scope.service" (which would all be non-global
 *                              zones), the system creates probes for every
 *                              global zone that hosts those instance.
 *
 *        "scope.checkFrom"     if specified, this field identifies the SAPI
 *        (optional string)     service for which probes will be created.  That
 *                              would normally be the same as "scope.service",
 *                              but in some cases, we monitor one service using
 *                              probes associated with another.  For example, we
 *                              monitor each storage zone with probes associated
 *                              with nameservice zones.
 *
 *                              Specifying this causes a probe to be generated
 *                              for each instance of "scope.service" for each
 *                              instance of "scope.checkFrom".  That is, it's
 *                              O(m * n) probes, where "m" and "n" are the
 *                              numbers of instances of the two services.
 *                              Generally, one or both of these services should
 *                              have a fixed, small number of instances (like
 *                              "nameservice" or "ops"), rather than both being
 *                              services that are intended to scale to large
 *                              numbers (like "webapi").
 *
 *    "checks"                  array of objects describing Amon probes to
 *    (required array)          create.  There will be one Amon probe generated
 *                              for each "check".  There must be at least one
 *                              check.
 *
 *        "checks[i].type"      See Amon probe's "type" field.
 *        (required string)
 *
 *        "checks[i].config"    See Amon probe's "config" field.
 *        (required object)
 *
 *                              The "type" and "config" properties of each check
 *                              are the same as for the corresponding properties
 *                              of Amon probes, except that we only support a
 *                              limited subset of types, and we support an
 *                              additional "config" property: probes of type
 *                              "cmd" may specify the special property:
 *
 *        "checks[i].config.autoEnv"    If specified, then this should
 *        (optional array of strings)   be an array of SAPI metadata variables.
 *                                      These metadata variables will be made
 *                                      available in the process environment of
 *                                      the command that runs as part of the
 *                                      probe.  That means you can use them in
 *                                      the probe's script.
 *
 *                                      When this property is specified on
 *                                      supported probes, the "env" property of
 *                                      the probe is filled in with the current
 *                                      values of the specified SAPI metadata,
 *                                      and the "autoEnv" property itself is not
 *                                      passed through to Amon.
 *
 *    "ka"                      Specified knowledge article content.  This is
 *    (required object)         prose text intended for operators trying to
 *                              understand an open alarm.
 *
 *        "ka.title"            A very short summary of the problem.  This
 *        (required string)     should fit comfortably in about 30 columns of
 *                              text.
 *
 *        "ka.severity"         The severity of the problem, which must be one
 *        (required string)     of the following:
 *
 *                                  "critical"  the data path or jobs path
 *                                              may be significantly affected or
 *                                              may be imminently so.
 *                                              "Affected" here means increased
 *                                              error rate or latency for end
 *                                              user operations.
 *
 *                                  "major"     the data path or jobs path may
 *                                              be affected, but the impact is
 *                                              likely minor or limited to a
 *                                              small number of requests.  Even
 *                                              if not currently affected, these
 *                                              paths are at risk for a major
 *                                              disruption.
 *
 *                                  "minor"     the data path and jobs path are
 *                                              likely not currently affected
 *                                              (not more than a bounded, fixed
 *                                              number of requests).
 *
 *                                  The severity is also used to determine the
 *                                  Amon contacts applied to the probe group.
 *                                  The specific contacts for each severity
 *                                  level are contained in the top-level
 *                                  configuration file.
 *
 *        "ka.description"      A summary of the problem in more detail than
 *        (required string)     "ka.title".
 *
 *        "ka.response"         A description of any automated response taken by
 *        (required string)     the system in response to this event.
 *
 *        "ka.impact"           A description of the impact to the system of
 *        (required string)     this event.  This is a good place to describe
 *                              how this event affects the error rate, latency,
 *                              or anything else that's affected by this event.
 *
 *        "ka.action"           A description of actions recommended for the
 *        (required string)     operator.
 *
 * To summarize, an Amon probe is created for each element of "checks" for each
 * instance of SAPI service "scope.service".   If "scope.checkFrom" is
 * specified, then all of _those_ probes are created for each instance of
 * the "checkFrom" service.
 *
 * Probe templates MAY also contain these top-level properties
 *
 *    "legacyName"              a string describing the legacy mantamon
 *    (string)                  probe names that correspond to the probes
 *                              for this template.  This is currently not used
 *                              by anything, but is potentially useful for
 *                              readers to understand the history of particular
 *                              templates.
 */

var assertplus = require('assert-plus');
var fs = require('fs');
var jsprim = require('jsprim');
var jsyaml = require('js-yaml');
var path = require('path');
var vasync = require('vasync');
var VError = require('verror');

var services = require('../services');

/* Exported interface */
exports.loadMetadata = loadMetadata;
exports.probeGroupNameForTemplate = probeGroupNameForTemplate;
exports.MetadataLoader = MetadataLoader;

/* Exposed for testing only */
exports.testingParseProbeGroupName = parseProbeGroupName;

/*
 * Concurrency with which we load probe template files.
 */
var PTS_CONCURRENCY_FILES = 10;

/*
 * Load all of the probe template metadata from the specified directory.
 *
 * Named arguments include:
 *
 *     directory	path to directory containing all probe template files
 *     (string)
 *
 * "callback" is invoked upon completion as callback(err, metadata).
 */
function loadMetadata(args, callback)
{
	var mdl;

	assertplus.object(args, 'args');
	assertplus.string(args.directory, 'args.directory');

	mdl = new MetadataLoader();
	mdl.loadFromDirectory(args.directory, function onLoadDone() {
		var error = VError.errorFromList(mdl.errors());
		callback(error, error === null ? mdl.mdl_amoncfg : null);
	});
}

/*
 * An instance of MantaAmonMetadata represents the local metadata associated
 * with probes and probe groups.  This is the primary exposed interface from
 * this module, though objects are only exposed through the loading interfaces.
 * (Outside consumers cannot create instances of this class directly.)
 */
function MantaAmonMetadata()
{
	/* Probe group information keyed by the configured event name. */
	this.mam_templates_byevent = {};

	/*
	 * A single template can be used to define multiple probe groups with
	 * the service name filled into the event name, which makes it different
	 * for each service.  For example, the "SMF maintenance" template
	 * has a scope of "each", which causes us to create one probe group per
	 * distinct service.  The event name in the template is:
	 *
	 *     upset.manta.$service.smf_maintenance
	 *
	 * This creates one event per distinct service, which look like:
	 *
	 *     upset.manta.postgres.smf_maintenance
	 *     upset.manta.moray.smf_maintenance
	 *     ...
	 *
	 * To be able to recognize these expanded names, we expand them as we
	 * process each template and store aliases here.
	 */
	this.mam_event_aliases = {};
}


/*
 * Public interfaces: for all callers
 */

/*
 * Public interface to return the knowledge article for an event called
 * "eventName".  Returns null if there is no knowledge article registered for
 * this event.
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.eventKa = function eventKa(eventName)
{
	var resolved = this.resolveEventName(eventName);
	if (resolved === null) {
		return (null);
	}

	return (this.mam_templates_byevent[resolved].pt_ka);
};

/*
 * Iterate the probe group events represented in this metadata.
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.eachEvent = function (func)
{
	jsprim.forEachKey(this.mam_templates_byevent, function (evt, pt) {
		if (pt.pt_aliases.length === 0) {
			func(evt);
		}
	});

	jsprim.forEachKey(this.mam_event_aliases, function (alias) {
		func(alias);
	});
};


/*
 * Semi-private interfaces: for other files in this directory.
 */

/*
 * Iterate all registered probe templates.
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.eachTemplate = function (func)
{
	jsprim.forEachKey(this.mam_templates_byevent, function (_, pt) {
		func(pt);
	});
};

/*
 * Given a probe group with name "probeGroupName", return the string name of the
 * event that is emitted when an alarm for this group fires.  This is primarily
 * useful for passing to the eventKa() function to get the knowledge article
 * associated with this probe group.  This function returns null if the event
 * name is unknown or not applicable (because it's an operator-created probe
 * group or the like).
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.probeGroupEventName =
    function probeGroupEventName(probeGroupName)
{
	var result;

	result = parseProbeGroupName(probeGroupName);
	if (result.error !== null || result.isLegacy || result.isOther) {
		return (null);
	}

	assertplus.string(result.eventName);
	return (result.eventName);
};

/*
 * Given a probe group with name "probeGroupName", determine whether it should
 * be removed as part of a configuration update operation.  See the block
 * comment at the top of this file for an explanation of why we mark different
 * types of groups for removal.
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.probeGroupIsRemovable =
    function probeGroupIsRemovable(probeGroupName)
{
	var result, eventName;

	result = parseProbeGroupName(probeGroupName);
	if (result.error !== null || result.isOther) {
		return (false);
	}

	if (result.isLegacy) {
		return (true);
	}

	assertplus.string(result.eventName);
	eventName = this.resolveEventName(result.eventName);
	return (eventName === null);
};


/*
 * Private interfaces: for this file only
 */

/*
 * Private interface to load a probe template into this data structure.  This
 * normally comes from the probe template files checked into this repository,
 * though the test suite can use this to load specific templates.
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.addTemplate = function addTemplate(args)
{
	var inp, eventName, pt, error, nsubs;
	var self = this;

	assertplus.object(args, 'args');
	assertplus.object(args.input, 'args.input');
	assertplus.string(args.originLabel, 'args.originLabel');

	inp = args.input;
	eventName = inp.event;

	if (this.mam_templates_byevent.hasOwnProperty(eventName)) {
		return (new VError('%s: re-uses event name "%s" previously ' +
		    'used in template "%s"', args.originLabel, eventName,
		    this.mam_templates_byevent[eventName].pt_origin_label));
	}

	pt = new ProbeTemplate({
	    'input': inp,
	    'originLabel': args.originLabel
	});

	if (pt.pt_scope.ptsc_service != 'each') {
		if (/[^a-zA-Z0-9_.]/.test(eventName)) {
			return (new VError('%s: event name contains ' +
			    'unsupported characters', args.originLabel));
		}

		this.mam_templates_byevent[eventName] = pt;
		return (null);
	}

	this.mam_templates_byevent[eventName] = pt;

	/*
	 * Generate per-service aliases for probe groups that generate more than
	 * one event name.
	 */
	nsubs = 0;
	error = null;
	services.mSvcNamesProbes.forEach(function (svcname) {
		var fmasvcname;

		/*
		 * For FMA event names, we adopt the convention of using
		 * underscores instead of dashes.  We need to translate service
		 * names accordingly.
		 */
		assertplus.ok(svcname !== 'marlin');
		fmasvcname = svcname.replace(/-/g, '_');

		/*
		 * We support certain limited expansions within the FMA event
		 * name.  Each expansion is an ASCII string beginning with '$',
		 * followed by an alphabetic character or "_", followed by any
		 * number of alphanumeric characters or "_".  This is perhaps
		 * simplistic, but because these fields are otherwise plaintext,
		 * there's nothing else to confuse the interpretation here
		 * (e.g., quoted strings or escape characters).  Note that the
		 * use of '$' or '$3' or the like will work fine, though a
		 * literal string like $ABC that is not intended to be expanded
		 * will not work.  This would be a little strange for an FMA
		 * event name.
		 */
		var aliasname = pt.pt_event.replace(
		    /\$([a-zA-Z_][a-zA-Z0-9_]*)/g,
		    function onMatch(substr, varname) {
			assertplus.equal('$' + varname, substr);
			if (varname == 'service') {
				nsubs++;
				return (fmasvcname);
			}

			if (error === null) {
				error = new VError('template "%s": unknown ' +
				    'variable "%s" in event name',
				    pt.pt_origin_label, substr);
			}

			return ('INVALID');
		    });


		if (/[^a-zA-Z0-9_.]/.test(aliasname)) {
			if (error === null) {
				error = new VError('%s: expanded event name ' +
				    'contains unsupported characters: "%s"',
				    args.originLabel, aliasname);
			}
		} else {
			pt.pt_aliases.push({
			    'pta_event': aliasname,
			    'pta_service': svcname
			});
		}
	});

	if (error === null && nsubs === 0) {
		return (new VError('template "%s": templates with scope ' +
		    '"each" must use "$service" in event name to ensure ' +
		    'uniqueness', pt.pt_origin_label));
	}

	if (error !== null) {
		return (error);
	}

	pt.pt_aliases.forEach(function (alias) {
		assertplus.ok(!self.mam_event_aliases.hasOwnProperty(
		    alias.pta_event), 'duplicate alias: ' + alias.pta_event);
		self.mam_event_aliases[alias.pta_event] = pt.pt_event;
	});

	return (null);
};

/*
 * Resolve an event name that may be an alias to the underlying event name.
 * Returns null if this event is not known in this metadata.
 *
 * See above for allowed callers.
 */
MantaAmonMetadata.prototype.resolveEventName = function (eventName)
{
	if (this.mam_event_aliases.hasOwnProperty(eventName)) {
		assertplus.ok(this.mam_templates_byevent.hasOwnProperty(
		    this.mam_event_aliases[eventName]));
		return (this.mam_event_aliases[eventName]);
	}

	if (this.mam_templates_byevent.hasOwnProperty(eventName)) {
		return (eventName);
	}

	return (null);
};


var schemaProbeTemplateFile = {
    'type': 'array',
    'required': true,
    'items': {
	'type': 'object',
	'additionalProperties': false,
	'properties': {
	    'event': {
	        'type': 'string',
		'required': true,
		'minLength': 'upset.manta.'.length
	    },
	    'legacyName': { 'type': 'string' },
	    'scope': {
		'type': 'object',
		'required': true,
		'additionalProperties': false,
		'properties': {
		    'service': {
		        'type': 'string',
			'required': true,
			'enum': [ 'each', 'all' ].concat(
			    services.mSvcNamesProbes)
		    },
		    'global': {
			'type': 'boolean'
		    },
		    'checkFrom': {
		        'type': 'string',
			'enum': services.mSvcNamesProbes
		    }
		}
	    },
	    'checks': {
		'type': 'array',
		'required': true,
		'minItems': 1,
		'items': {
		    'type': 'object',
		    'additionalProperties': false,
		    'properties': {
			'type': {
			    'type': 'string',
			    'enum': [
			        'bunyan-log-scan',
				'cmd',
				'disk-usage',
				'log-scan'
			    ]
			},
			'config': {
			    'type': 'object'
			}
		    }
		}
	    },
	    'ka': {
		'type': 'object',
		'required': true,
		'additionalProperties': false,
		'properties': {
		    'title': { 'type': 'string', 'required': true },
		    'description': { 'type': 'string', 'required': true },
		    'severity': { 'type': 'string', 'required': true },
		    'response': { 'type': 'string', 'required': true },
		    'impact': { 'type': 'string', 'required': true },
		    'action': { 'type': 'string', 'required': true }
		}
	    }
	}
    }
};


/*
 * This class is used as a struct, with details private to this subsystem.
 * The fields here closely mirror those in the probe template schema.  For
 * details, see the documentation for that.
 *
 * The constructor takes arguments in the form as it comes out of the the
 * YAML-parsed files.  These structures should have already been validated.
 */
function ProbeTemplate(args)
{
	var self = this;
	var inp;

	assertplus.object(args, 'args');
	assertplus.object(args.input, 'args.input');
	assertplus.string(args.originLabel, 'args.originLabel');

	inp = args.input;

	/*
	 * The origin label is a string describing the source of this template.
	 * It's generally a filename and potentially an index into the templates
	 * listed in the file.  This is used in error messages that result from
	 * building a configuration based on this template.
	 */
	this.pt_origin_label = args.originLabel;

	/* FMA-style event class for this probe template. */
	this.pt_event = inp.event;

	/*
	 * The scope object describes which components this probe monitors (and
	 * potentially from which other components, if those are different).
	 */
	this.pt_scope = {};
	this.pt_scope.ptsc_service = inp.scope.service;
	this.pt_scope.ptsc_global = (inp.scope.global === true);
	this.pt_scope.ptsc_check_from = inp.scope.checkFrom || null;

	this.pt_checks = [];
	inp.checks.forEach(function (c) {
		var cc;

		cc = {};
		cc.ptc_type = c.type;
		cc.ptc_config = jsprim.deepCopy(c.config);
		self.pt_checks.push(cc);
	});

	this.pt_ka = {};
	this.pt_ka.ka_title = inp.ka.title;
	this.pt_ka.ka_description = inp.ka.description;
	this.pt_ka.ka_severity = inp.ka.severity;
	this.pt_ka.ka_response = inp.ka.response;
	this.pt_ka.ka_impact = inp.ka.impact;
	this.pt_ka.ka_action = inp.ka.action;
	this.pt_aliases = [];
}


/*
 * Represents the operation of loading a bunch of probe templates from
 * configuration files.
 */
function MetadataLoader()
{
	/* problems encountered during load */
	this.mdl_load_errors = [];

	/* probe templates found */
	this.mdl_amoncfg = new MantaAmonMetadata();

	/* for debugging only */
	this.mdl_load_pipeline = null;
}

/*
 * Read YAML files in "directory" and load them.  Invokes "callback" upon
 * completion.  Errors and warnings are not passed to the callback.  See the
 * separate public methods for accessing those.
 */
MetadataLoader.prototype.loadFromDirectory =
    function loadFromDirectory(directory, callback)
{
	var files;
	var queue;

	assertplus.string(directory, 'directory');
	assertplus.func(callback, 'callback');

	this.mdl_load_pipeline = vasync.pipeline({
	    'arg': this,
	    'funcs': [
		function listDirectory(self, subcallback) {
			fs.readdir(directory,
			    function onReaddirDone(err, entries) {
				if (err) {
					err = new VError(err, 'readdir "%s"',
					    directory);
					self.mdl_load_errors.push(err);
					subcallback();
					return;
				}

				files = entries.filter(function (e) {
					return (jsprim.endsWith(e, '.yaml'));
				}).map(function (e) {
					return (path.join(directory, e));
				});

				subcallback();
			    });
		},

		function readFiles(self, subcallback) {
			if (self.mdl_load_errors.length > 0) {
				setImmediate(subcallback);
				return;
			}

			queue = vasync.queuev({
			    'concurrency': PTS_CONCURRENCY_FILES,
			    'worker': function loadQueueCallback(f, qcallback) {
				self.loadFromFile(f, qcallback);
			    }
			});

			files.forEach(function (f) { queue.push(f); });
			queue.on('end', function () { subcallback(); });
			queue.close();
		}
	    ]
	}, function (err) {
		/*
		 * Errors should be pushed onto mdl_load_errors, not emitted
		 * here.
		 */
		assertplus.ok(!err);
		callback();
	});
};

/*
 * Read a single YAML file and load it.  Invokes "callback" upon completion.
 * Like loadFromDirectory(), errors and warnings are not passed to the callback,
 * but recorded for later.
 */
MetadataLoader.prototype.loadFromFile =
    function loadFromFile(filename, callback)
{
	var self = this;
	var readoptions;

	assertplus.string(filename, 'filename');
	assertplus.func(callback, 'callback');

	readoptions = { 'encoding': 'utf8' };
	fs.readFile(filename, readoptions, function (err, contents) {
		if (err) {
			err = new VError(err, 'read "%s"', filename);
			self.mdl_load_errors.push(err);
		} else {
			self.loadFromString(contents, filename);
		}

		callback();
	});
};

MetadataLoader.prototype.loadFromString =
    function loadFromString(contents, inputlabel)
{
	var parsed, err;
	var self = this;

	assertplus.string(contents, 'contents');
	assertplus.string(inputlabel, 'inputlabel');

	try {
		parsed = jsyaml.safeLoad(contents, {
		    'filename': inputlabel
		});
	} catch (ex) {
		err = new VError(ex, 'parse "%s"', inputlabel);
		self.mdl_load_errors.push(err);
		return;
	}

	err = jsprim.validateJsonObject(schemaProbeTemplateFile, parsed);
	if (err instanceof Error) {
		err = new VError(err, 'parse "%s"', inputlabel);
		self.mdl_load_errors.push(err);
		return;
	}

	parsed.forEach(function (p, i) {
		var label, error, k;

		label = inputlabel + ': probe ' + (i + 1);

		/*
		 * We require event names to begin with "upset.manta."
		 */
		if (!jsprim.startsWith(p.event, 'upset.manta.')) {
			self.mdl_load_errors.push(new VError(
			    '%s: field "event": must begin with "upset.manta."',
			    label));
			return;
		}

		/*
		 * In order to format the various messages, it's
		 * important that they be consistent with respect to
		 * trailing newlines.  It's easy to get this wrong in
		 * YAML, so we check for it here.
		 */
		for (k in p.ka) {
			if (jsprim.endsWith(p.ka[k], '\n')) {
				error = new VError('%s: field ka.%s: ' +
				    'ends with trailing newline',
				    label, k);
				break;
			}
		}

		if (!error) {
			error = self.mdl_amoncfg.addTemplate({
			    'input': p,
			    'originLabel': label
			});
		}

		if (error) {
			self.mdl_load_errors.push(error);
		}
	});
};

MetadataLoader.prototype.errors = function ()
{
	return (this.mdl_load_errors.slice());
};

/*
 * List of unversioned probe group names used by previous versions of this
 * software.
 */
var MAM_LEGACY_PROBEGROUP_NAMES = [
    'authcache-alert',
    'compute-alert',
    'electric-moray-alert',
    'jobsupervisor-alert',
    'loadbalancer-alert',
    'moray-alert',
    'nameservice-alert',
    'ops-alert',
    'ops-info',
    'postgres-alert',
    'storage-alert',
    'webapi-alert'
];

/*
 * Probe group names
 *
 * Probe templates are defined in the source code configuration.  Each template
 * is expected to correspond to a distinct failure mode.  There may be more than
 * one probe group for each template, depending on the scope.  These probe
 * groups need to have names, and those names link them to the metadata we have
 * (i.e., the knowledge articles).  To do this, we use FMA-style event names
 * (e.g., upset.manta.$service.$problem).  Since this information will be
 * programmatically parsed, we want to include a version number.  Together, we
 * construct the probe group name for a given template by taking the FMA-style
 * event name, substituting the service name if requested, and appending a
 * version suffix.
 *
 * Given an arbitrary probe group name, we can classify it into one of a few
 * buckets:
 *
 *    - If it matches one of the well-known probe groups used by previous
 *      versions of this software, we call that "legacy".  We don't have
 *      metadata about these groups, and they should be removed if we're making
 *      updates to the probe configuration.
 *
 *    - Otherwise, if we cannot find the ";v=" suffix, then we assume this not a
 *      probe created by this software.  This is likely something operators
 *      created.  We'll generally leave these alone.
 *
 *    - Otherwise, if we find the suffix, but the version is newer than one we
 *      recognize, then we'll not touch this probe group.  In the future, if we
 *      decide to change the encoding (e.g., to include additional information
 *      in the probe group name), then we can do so as long as we preserve a
 *      ";v=" suffix with a new version number.
 *
 *    - Finally, if we find an acceptable version suffix, then this is a probe
 *      group that we know how to manage.
 *
 * See the block comment at the top of this file for details on these different
 * kinds of probe groups.
 */

function probeGroupNameForTemplate(pt, eventname)
{
	assertplus.object(pt, 'pt');
	assertplus.string(eventname, 'eventname');
	return (eventname + ';v=1');
}

function parseProbeGroupName(probeGroupName)
{
	var result, i, verpart;

	result = {};
	result.error = null;		/* failure to parse (bad version) */
	result.isLegacy = null;		/* from "mantamon" era */
	result.isOther = null;		/* operator-created */
	result.eventName = null;	/* software-created, versioned era */

	if (MAM_LEGACY_PROBEGROUP_NAMES.indexOf(probeGroupName) != -1) {
		result.isLegacy = true;
		result.isOther = false;
		return (result);
	}

	result.isLegacy = false;
	i = probeGroupName.indexOf(';');
	if (i == -1 || probeGroupName.substr(i + 1, 2) != 'v=') {
		result.isOther = true;
		return (result);
	}

	result.isOther = false;
	verpart = probeGroupName.substr(i + 3);
	if (verpart != '1') {
		result.error = new VError('unrecognized version "%s" in ' +
		    'probe group with name "%s"', verpart, probeGroupName);
		return (result);
	}

	result.eventName = probeGroupName.slice(0, i);
	return (result);
}
