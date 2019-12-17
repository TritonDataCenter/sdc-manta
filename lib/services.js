/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
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
exports.serviceIsExperimental = serviceIsExperimental;
exports.serviceConfigProperties = serviceConfigProperties;
exports.serviceNameToImageNames = serviceNameToImageNames;

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
    'ops',
    'madtom',
    'rebalancer',
    'reshard',
    'pgstatsmon',
    'garbage-collector',
    'prometheus',
    'buckets-api',
    'buckets-postgres',
    'boray',
    'electric-boray'
];

/*
 * Services that might still exist in SAPI but are no longer used. Here just so
 * we don't blow up when we see an unexpected service.
 */
var mLegacySvcNames = [
    'marlin',
    'jobpuller',
    'jobsupervisor',
    'marlin-dashboard',
    'medusa'
];

/*
 * "Experimental" services -- that is, services for which deployment is allowed
 * only if the operator uses the "--experimental" flag with`manta-adm update`.
 */
var mSvcNamesExperimental = [
    'boray',
    'buckets-api',
    'buckets-postgres',
    'electric-boray'
];

/*
 * Defines configuration properties for all services.
 */
var mSvcConfigs = {
    nameservice: {
        sharded: false,
        imageNames: ['mantav2-nameservice', 'manta-nameservice']
    },
    postgres: {
        sharded: true,
        imageNames: ['mantav2-postgres', 'manta-postgres']
    },
    moray: {
        sharded: true,
        imageNames: ['mantav2-moray', 'manta-moray']
    },
    'electric-moray': {
        sharded: false,
        imageNames: ['mantav2-electric-moray', 'manta-electric-moray']
    },
    storage: {
        sharded: false,
        imageNames: ['mantav2-storage', 'manta-storage']
    },
    authcache: {
        sharded: false,
        imageNames: ['mantav2-authcache', 'manta-authcache']
    },
    webapi: {
        sharded: false,
        imageNames: ['mantav2-webapi', 'manta-webapi']
    },
    loadbalancer: {
        sharded: false,
        imageNames: ['mantav2-loadbalancer', 'manta-loadbalancer']
    },
    ops: {
        sharded: false,
        imageNames: ['mantav2-ops', 'manta-ops']
    },
    madtom: {
        sharded: false,
        imageNames: ['mantav2-madtom', 'manta-madtom']
    },
    rebalancer: {
        sharded: false,
        imageNames: ['mantav2-rebalancer']
    },
    reshard: {
        sharded: false,
        imageNames: ['mantav2-reshard', 'manta-reshard']
    },
    pgstatsmon: {
        sharded: false,
        imageNames: ['mantav2-pgstatsmon', 'manta-pgstatsmon']
    },
    'garbage-collector': {
        sharded: false,
        imageNames: ['mantav2-garbage-collector', 'manta-garbage-collector']
    },
    prometheus: {
        sharded: false,
        imageNames: ['mantav2-prometheus', 'manta-prometheus']
    },
    'buckets-api': {
        sharded: false,
        imageNames: ['mantav2-buckets-api', 'manta-buckets-api']
    },
    'buckets-postgres': {
        sharded: true,
        /*
         * Note that the image name does not follow the naming scheme of
         * for the other services, because the buckets-postgres service
         * uses the same image as the postgres service.
         */
        imageNames: ['mantav2-postgres', 'manta-postgres']
    },
    boray: {
        sharded: true,
        imageNames: ['mantav2-boray', 'manta-boray']
    },
    'electric-boray': {
        sharded: false,
        imageNames: ['mantav2-electric-boray', 'manta-electric-boray']
    }
};

exports.mSvcNames = mSvcNames;

/*
 * This is exposed for testing only!  There are functional interfaces for
 * accessing service configuration properties.
 */
exports.mSvcConfigsPrivate = mSvcConfigs;

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
function ServiceConfiguration(keys) {
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
ServiceConfiguration.prototype.each = function(callback) {
    var self = this;
    this.sc_counts.forEach(function(config) {
        callback(
            config,
            self.sc_keys.map(function(k) {
                return config[k];
            })
        );
    });
};

/*
 * Sort configuration by specified column. "c" is an array of comparators
 * e.g. "SERVICE", "SH", "ZONENAME", "callback" is the same object as in
 * the `each` function above, called for each configuration -- the
 * comparators (keys) and their corresponding values.
 */
ServiceConfiguration.prototype.eachSorted = function(c, callback) {
    var self = this;
    var sc_counts_dup = this.sc_counts.slice();
    common.sortObjectsByProps(sc_counts_dup, c);
    sc_counts_dup.forEach(function(config) {
        callback(
            config,
            self.sc_keys.map(function(k) {
                return config[k];
            })
        );
    });
};

/*
 * Get the count of services having the specified configuration.  "config"
 * should be an object with the same fields passed in the constructor.
 */
ServiceConfiguration.prototype.get = function(config) {
    var i, k;
    var key, row;
    for (i = 0; i < this.sc_counts.length; i++) {
        row = this.sc_counts[i];
        for (k = 0; k < this.sc_keys.length; k++) {
            key = this.sc_keys[k];
            if (config[key] !== row[key]) {
                break;
            }
        }

        if (k === this.sc_keys.length) {
            return row['count'];
        }
    }

    return 0;
};

/*
 * Returns true if this object has a value for the given configuration.
 */
ServiceConfiguration.prototype.has = function(config) {
    var i, k;
    var key, row;
    for (i = 0; i < this.sc_counts.length; i++) {
        row = this.sc_counts[i];
        for (k = 0; k < this.sc_keys.length; k++) {
            key = this.sc_keys[k];
            if (config[key] !== row[key]) {
                break;
            }
        }

        if (k === this.sc_keys.length) {
            return true;
        }
    }

    return false;
};

/*
 * Increment the count of instances associated with configuration "config" by
 * value "count".
 */
ServiceConfiguration.prototype.incr = function(config, count) {
    var i, k;
    var key, row;

    if (arguments.length === 1) {
        count = 1;
    }

    for (i = 0; i < this.sc_counts.length; i++) {
        row = this.sc_counts[i];
        for (k = 0; k < this.sc_keys.length; k++) {
            key = this.sc_keys[k];
            if (config[key] !== row[key]) {
                break;
            }
        }

        if (k === this.sc_keys.length) {
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
ServiceConfiguration.prototype.summary = function() {
    var rv = {};
    this.each(function(row, rowkey) {
        common.insert(rv, row['count'], rowkey);
    });
    return rv;
};

/*
 * Given a service name, return whether that service is sharded.
 */
function serviceIsSharded(svcname) {
    assertplus.ok(
        serviceNameIsValid(svcname),
        'service "' + svcname + '" is not valid'
    );
    if (mLegacySvcNames.indexOf(svcname) !== -1) {
        return false;
    }
    return mSvcConfigs[svcname].sharded;
}

/*
 * Given a service name, return whether that service is experimental.
 */
function serviceIsExperimental(svcname) {
    assertplus.ok(serviceNameIsValid(svcname));
    return mSvcNamesExperimental.indexOf(svcname) !== -1;
}

/*
 * Given a service name, return an array of the properties typically used to
 * group instances of this service.  For most services, this is just the "image"
 * property -- instances should be grouped by their image version alone.  For
 * sharded services, the shard number is also relevant.
 */
function serviceConfigProperties(svcname) {
    return serviceIsSharded(svcname) ? ['SH', 'IMAGE'] : ['IMAGE'];
}

/*
 * Returns true if the given string is the name of a Manta service.
 */
function serviceNameIsValid(svcname) {
    assertplus.string(svcname);
    return (
        mSvcConfigs.hasOwnProperty(svcname) ||
        mLegacySvcNames.indexOf(svcname) !== -1
    );
}

/*
 * Returns the expected names for images deployed under the given service.
 */
function serviceNameToImageNames(svcname) {
    assertplus.ok(serviceNameIsValid(svcname));
    return mSvcConfigs[svcname].imageNames;
}
