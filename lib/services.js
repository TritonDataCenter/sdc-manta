/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * lib/services.js: interfaces for working with deployed Manta services.  Much
 * service-related functionality still resides in other files inside this
 * module, but common pieces should wind up here.
 */

/*
 * Be careful about what other modules are included here.  It would be best to
 * avoid cyclic dependencies with other files in this package.
 */
var assertplus = require('assert-plus');
var jsprim = require('jsprim');
var common = require('./common');

/* Public interface (used only within this module). */
exports.ServiceConfiguration = ServiceConfiguration;
exports.serviceNameIsValid = serviceNameIsValid;
exports.serviceIsSharded = serviceIsSharded;
exports.serviceSupportsOneach = serviceSupportsOneach;
exports.serviceSupportsProbes = serviceSupportsProbes;
exports.serviceConfigProperties = serviceConfigProperties;
exports.serviceSortConfigProps = serviceSortConfigProps;

/*
 * Service names deployed by default, in the order they get deployed.  This must
 * be kept in sync with mSvcConfigs below and the various parameters in
 * lib/layout.js.
 */
var mSvcNames = [
    'nameservice',
    'postgres',
    'moray',
    'electric-moray',
    'storage',
    'authcache',
    'webapi',
    'loadbalancer',
    'jobsupervisor',
    'jobpuller',
    'medusa',
    'ops',
    'madtom',
    'marlin-dashboard',
    'marlin',
    'reshard'
];

/*
 * Defines configuration properties for all services.  We do not allow
 * manta-oneach to be used with "marlin" zones.
 */
var mSvcConfigs = {
    'nameservice':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'postgres':		{ 'oneach': true,  'probes': true,  'sharded': true  },
    'moray':		{ 'oneach': true,  'probes': true,  'sharded': true  },
    'electric-moray':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'storage':		{ 'oneach': true,  'probes': true,  'sharded': false },
    'authcache':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'webapi':		{ 'oneach': true,  'probes': true,  'sharded': false },
    'loadbalancer':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'jobsupervisor':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'jobpuller':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'medusa':		{ 'oneach': true,  'probes': true,  'sharded': false },
    'ops':		{ 'oneach': true,  'probes': true,  'sharded': false },
    'madtom':		{ 'oneach': true,  'probes': true,  'sharded': false },
    'marlin-dashboard':	{ 'oneach': true,  'probes': true,  'sharded': false },
    'reshard':		{ 'oneach': true,  'probes': true,  'sharded': false },
    'marlin':		{ 'oneach': false, 'probes': false, 'sharded': false },
    'propeller':	{ 'oneach': true,  'probes': false, 'sharded': false }
};

var mSvcConfigSortProps = {
    'NETWORKS': [ 'ipv4_uuid' ]
};

/*
 * Properties differ slightly depending on whether they are the internal
 * representation of the service or the definition from the config, so here we
 * provide the translations.
 */
var mSvcPropertyTranslations = {
    'SH': 'shard',
    'IMAGE': 'image_uuid',
    'NETWORKS': 'untrusted_networks'
};

var mSvcProperties = Object.keys(mSvcPropertyTranslations);

exports.mSvcNames = mSvcNames;
exports.mSvcNamesProbes = mSvcNames.filter(serviceSupportsProbes);
exports.mSvcPropertyTranslations = mSvcPropertyTranslations;

/*
 * These are exposed for testing only!  There are functional interfaces for
 * accessing service configuration properties.
 */
exports.mSvcConfigsPrivate = mSvcConfigs;
exports.mSvcPropertiesPrivate = mSvcProperties;


/*
 * A "ServiceConfiguration" describes a count of service instances, grouped
 * based on unique "configurations" for that service.  For most services, the
 * "configuration" is just the image version for each instance.  This results in
 * a count of instances at each version.  For sharded services like "postgres"
 * and "moray", the "configuration" consists of both the shard number and image,
 * so the resulting counts are kept per (shard, image) tuple.  This class is
 * agnostic to what fields are part of the configuration.  Those are specified
 * in the constructor, as in:
 *
 *     svccfg = new ServiceConfiguration([ "IMAGE", "SH" ]);
 *
 * When you use each() to iterate the various configurations, you'll receive an
 * object describing each particular configuration, such as:
 *
 *     { "IMAGE": ..., "SH": 2 }
 *
 * which denotes a given image for shard 2.  Similarly, when you modify the
 * count using incr(), you must provide an object like the above one that
 * specifies a particular configuration whose count you're updating.
 *
 * "manta-adm update" uses several different ServiceConfiguration objects.
 * There may be a ServiceConfiguration per service per compute node that tracks
 * the number of instances of that service on each compute node, as well as a
 * group of fleet-wide per-service objects that track the total number of
 * instances of each service (and version, and shard) across the fleet.
 *
 * In general, we don't expect to have many configurations for a given service,
 * so we store this as a flat array.
 */
function ServiceConfiguration(keys)
{
	assertplus.ok(keys.length > 0);
	this.sc_keys = keys;
	this.sc_counts = [];
}

/*
 * Iterate the distinct configurations having non-zero values.  "callback" is
 * invoked for each one as callback(config, values), where "config" is an object
 * with key-value pairs for this configuration (e.g., "IMAGE" and "SH"), and
 * "values" is an array of the values for each of these fields (e.g., the
 * specific image and shard).
 */
ServiceConfiguration.prototype.each = function (callback)
{
	var self = this;
	this.sc_counts.forEach(function (config) {
		callback(config,
		    self.sc_keys.map(function (k) { return (config[k]); }));
	});
};

/*
 * Sort configuration by specified column. "c" is an array of comparators
 * e.g. "SERVICE", "SH", "ZONENAME", "callback" is the same object as in
 * the `each` function above, called for each configuration -- the
 * comparators (keys) and their corresponding values.
 */
ServiceConfiguration.prototype.eachSorted = function (c, callback)
{
	var self = this;
	var sc_counts_dup = this.sc_counts.slice();
	common.sortObjectsByProps(sc_counts_dup, c);
	sc_counts_dup.forEach(function (config) {
		callback(config, self.sc_keys.map(function (k) {
			return (config[k]); }));
	});
};

/*
 * Get the count of services having the specified configuration.  "config"
 * should be an object with the same fields passed in the constructor.
 */
ServiceConfiguration.prototype.get = function (config)
{
	var i, k;
	var key, row;
	for (i = 0; i < this.sc_counts.length; i++) {
		row = this.sc_counts[i];
		for (k = 0; k < this.sc_keys.length; k++) {
			key = this.sc_keys[k];
			if (!configsEqual(config[key], row[key], key))
				break;
		}

		if (k == this.sc_keys.length)
			return (row['count']);
	}

	return (0);
};

/*
 * Returns true if this object has a value for the given configuration.
 */
ServiceConfiguration.prototype.has = function (config)
{
	var i, k;
	var key, row;
	for (i = 0; i < this.sc_counts.length; i++) {
		row = this.sc_counts[i];
		for (k = 0; k < this.sc_keys.length; k++) {
			key = this.sc_keys[k];
			if (!configsEqual(config[key], row[key], key))
				break;
		}

		if (k == this.sc_keys.length)
			return (true);
	}

	return (false);
};

/*
 * Increment the count of instances associated with configuration "config" by
 * value "count".
 */
ServiceConfiguration.prototype.incr = function (config, count)
{
	var i, k;
	var key, row;

	if (arguments.length == 1)
		count = 1;

	for (i = 0; i < this.sc_counts.length; i++) {
		row = this.sc_counts[i];
		for (k = 0; k < this.sc_keys.length; k++) {
			key = this.sc_keys[k];
			if (!configsEqual(config[key], row[key], key))
				break;
		}

		if (k == this.sc_keys.length) {
			row['count'] += count;
			return;
		}
	}

	var obj = jsprim.deepCopy(config);
	obj['count'] = count;
	this.sc_counts.push(obj);
};

/*
 * Produce a plain-old-JavaScript-object summary (suitable for passing to
 * JSON.stringify()) of this service configuration.  Callers have committed to
 * this format, so care must be taken in changing this format.
 */
ServiceConfiguration.prototype.summary = function ()
{
	var rv, entry;
	var self = this;

	rv = [];

	this.each(function (row, rowkey) {
		entry = {};
		self.sc_keys.forEach(function (k) {
			var t = mSvcPropertyTranslations[k];
			if (row[k] !== '-')
				entry[t] = row[k];
		});
		entry['count'] = row['count'];
		rv.push(entry);
	});
	return (rv);
};

ServiceConfiguration.prototype.legacySummary = function ()
{
	var rv = {};
	this.each(function (row, rowkey) {
		common.insert(rv, row['count'], rowkey);
	});
	return (rv);
};

/*
 * Given a service name, return whether that service is sharded.
 */
function serviceIsSharded(svcname)
{
	assertplus.ok(serviceNameIsValid(svcname));
	return (mSvcConfigs[svcname].sharded);
}

/*
 * Given a service name, return an array of the properties typically used to
 * group instances of this service.  For most services, this is just the "image"
 * property -- instances should be grouped by their image version alone.  For
 * sharded services, the shard number is also relevant.
 */
function serviceConfigProperties(svcname, version)
{
	assertplus.optionalNumber(version);

	var properties;

	if (!version || version > 1) {
		properties = mSvcProperties.slice();
	} else {
		properties = [ 'SH', 'IMAGE' ];
	}

	if (!serviceIsSharded(svcname))
		properties.splice(mSvcProperties.indexOf('SH'), 1);

	return (properties);
}

/*
 * Returns true if the given string is the name of a Manta service.
 */
function serviceNameIsValid(svcname)
{
	assertplus.string(svcname);
	return (mSvcConfigs.hasOwnProperty(svcname));
}

/*
 * Returns true if the given service can be used with manta-oneach(1)
 */
function serviceSupportsOneach(svcname)
{
	assertplus.ok(serviceNameIsValid(svcname));
	return (mSvcConfigs[svcname].oneach);
}

/*
 * Returns true if the given service can support Amon probes.
 */
function serviceSupportsProbes(svcname)
{
	assertplus.ok(serviceNameIsValid(svcname));
	return (mSvcConfigs[svcname].probes);
}

/*
 * Takes a service config object and the key for that object and sorts its
 * properties, if applicable.
 */
function serviceSortConfigProps(config, key)
{
	if (typeof (config) === 'object' && 'sort' in config)
		config = common.sortObjectsByProps(config,
		    mSvcConfigSortProps[key] || []);

	return (config);
}

/*
 * Normalizes and compares 2 config objects and returns true if they match.
 */
function configsEqual(c1, c2, key)
{
	[ c1, c2 ].forEach(function (config) {
		config = serviceSortConfigProps(config, key);
	});

	if (jsprim.deepEqual(c1, c2))
		return (true);

	return (false);
}
