/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/layout.js: interfaces for laying out Manta services on a set of compute
 * nodes.
 *
 * The pattern for generating a layout of Manta services is:
 *
 *     o You start with a description of the desired deployment, including the
 *       available servers, their intended roles, how they're organized into
 *       datacenters and racks, and how many metadata shards should be deployed.
 *       The specific format is documented in manta-adm(1).  This description
 *       is usually either stored as JSON in a file or constructed directly in
 *       memory.
 *
 *     o Instantiate a DcConfigLoader() and call either loadFromFile() (if
 *       loading the description from a file) or loadDirectly() (if loading from
 *       a plain-old-JavaScript-object).  These methods produce a DcConfig
 *       object that represents the parsed, validated state.
 *
 *     o Invoke generateLayout() using the DcConfig.  This produces a Layout
 *       that you can serialize to an output stream.  You can then pass this to
 *       "manta-adm update" to deploy the configuration.
 *
 * This is used by the "manta-adm genconfig --from-file" command.
 */

var assertplus = require('assert-plus');
var fs = require('fs');
var jsprim = require('jsprim');
var tab = require('tab');
var vasync = require('vasync');
var VError = require('verror').VError;

var common = require('./common');
var services = require('./services');

/* Public interface */
exports.DcConfigLoader = DcConfigLoader;
exports.generateLayout = generateLayout;

/*
 * The parameters below configure broadly how we design a layout of Manta
 * services for a given set of availability zones, racks, and servers.
 */

/*
 * Exact-count services.  These services will be deployed with the exact
 * instance counts listed and striped across racks and servers.
 */
var ML_SERVICES_EXACT = {
	/*
	 * "nameservice" always gets three instances.  We can operate with five,
	 * but it's not clear this provides increased resilience.
	 */
	'nameservice': 3,

	/*
	 * The "ops" zone must be deployed only once within a region.  "madtom"
	 * and "marlin-dashboard" don't really benefit from more than one
	 * instance.
	 */
	'ops': 1,
	'madtom': 1,
	'marlin-dashboard': 1,

	/*
	 * "jobsupervisor", "jobpuller", and "medusa" are all involved with job
	 * execution.  We want more than one instance for availability, but we
	 * generally don't need much more than two for capacity.
	 */
	'jobsupervisor': 2,
	'jobpuller': 2,
	'medusa': 2,

	/*
	 * "propeller" is a testing component not intended for use in production
	 * environments.
	 */
	'propeller': 0
};

/*
 * Per-shard services.  The user tells us how many shards they want because
 * that's largely a function of desired operation capacity.  We deploy 3
 * instances of each service per shard.  (Three is pretty fundamental to the way
 * Manatee works, and it's a reasonable ratio for Moray as well.)  We'll attempt
 * to minimize colocating instances of the same service and the same shard and
 * maximize the number of metadata servers we use for this.
 */
var ML_NPERSHARD_INSTANCES = 3;
var ML_SERVICES_PER_SHARD = {
    'postgres': ML_NPERSHARD_INSTANCES,
    'moray': ML_NPERSHARD_INSTANCES
};

/*
 * Front door services.  The values in ML_FRONTDOOR_RATIOS represent ratios of
 * each of these service instances.  We'll multiply these out until we have one
 * instance of the largest-count services on each metadata server.  The ratios
 * below are heuristics based loosely on experience, not on rigorous
 * measurements of the relative capacities of the various services.
 */
var ML_FRONTDOOR_NMAXINSTANCES = 8;
var ML_FRONTDOOR_RATIOS = {
    'authcache': 1,
    'electric-moray': ML_FRONTDOOR_NMAXINSTANCES,
    'webapi': ML_FRONTDOOR_NMAXINSTANCES,
    'loadbalancer': ML_FRONTDOOR_NMAXINSTANCES
};

/*
 * Storage zones are deployed one-per-storage-server.  This is coded into the
 * implementation, with no configurable parameters.
 *
 * Compute zones are allocated on storage servers based on the percentage of
 * DRAM that should be used for all compute zones and the amount of DRAM that
 * each zone gets by default.  The values here err on the side of using too
 * little DRAM on the grounds that it's easy to provision more compute zones
 * later.  Note that Marlin will also use some DRAM (not accounted for here) as
 * a slop pool that can be allocated to any compute zone.  See the agent's
 * "zoneMemorySlopPercent" configuration parameter.  Additionally, the per-zone
 * DRAM number used here should match the agent's
 * "zoneDefaults.max_physical_memory" and "zoneDefaults.max_swap" configuration
 * parameters .
 *
 * Even for deployments that don't intend to make much use of jobs, Manta itself
 * uses them for garbage collection, auditing, and the like, so we need a few
 * zones on each storage server to accommodate those.
 */
var ML_COMPUTE_DRAM_PERCENT = 0.25;
var ML_COMPUTE_DRAM_DEFAULT = 1024;	/* megabytes */
var ML_COMPUTE_NMIN = 4;		/* min compute zones per server */

/*
 * When servers are specified without a rack or AZ, we assign them to a default
 * one to keep the implementation simple.
 */
var ML_DEFAULT_AZ = 'default_az';
var ML_DEFAULT_RACK = 'default_rack';

/*
 * JSON schema for the server configuration file.  This is described in
 * manta-adm(1).
 */
var ML_SCHEMA = {
    'type': 'object',
    'additionalProperties': false,
    'properties': {
	'nshards': {
	    'required': true,
	    'type': 'integer',
	    'minimum': 1,
	    'maximum': 1024
	},
	'images': {
	    'type': 'object'
	},
	'servers': {
	    'required': true,
	    'type': 'array',
	    'minItems': 1,
	    'items': {
	        'type': 'object',
		'additionalProperties': false,
		'properties': {
		    'type': {
		        'type': 'string',
			'required': true,
			'enum': [ 'metadata', 'storage' ]
		    },
		    'uuid': {
		        'type': 'string',
			'required': true,
			'minLength': 1
		    },
		    'memory': {
			'type': 'integer',
			'required': true,
			'minimum': 1,
			'maximum': 1024
		    },
		    'az': {
		        'type': 'string',
			'minLength': 1
		    },
		    'rack': {
		        'type': 'string',
			'minLength': 1
		    }
		}
	    }
	}
    }
};

/*
 * Generate a Layout from the given datacenter configuration.  Returns a Layout
 * object.  This operation cannot fail, but it can return a Layout that's
 * unpopulated except for the list of fatal errors.
 *
 *     dcconfig		instance of a DcConfig, which describes the desired
 *     			parameters of the deployment (including the servers
 *     			available and the number of shards).  You get this using
 *     			a DcConfigLoader.
 *
 *     images		mapping of service names to the image uuid to use for
 *     			each service
 */
function generateLayout(args)
{
	var dcconfig, images, layout, extrametadata, extrastorage;

	assertplus.object(args, 'args');
	assertplus.object(args.dcconfig, 'args.dcconfig');
	assertplus.object(args.images, 'args.images');

	dcconfig = args.dcconfig;
	assertplus.number(dcconfig.dc_nshards);
	assertplus.ok(dcconfig.dc_az_names.length > 0);
	assertplus.ok(dcconfig.dc_rack_names.length > 0);
	assertplus.ok(dcconfig.dc_server_names.length > 0);

	/*
	 * We have a default set of images passed in by the caller, but these
	 * may be overridden by the images specified in the configuration file.
	 * This is primarily useful for getting consistent output files for
	 * testing purposes.
	 */
	images = jsprim.deepCopy(args.images);
	jsprim.forEachKey(dcconfig.dc_images, function (svcname, image) {
		images[svcname] = image;
	});

	layout = new Layout(dcconfig);

	if (dcconfig.dc_servers_metadata.length === 0 ||
	    dcconfig.dc_servers_storage.length === 0) {
		layout.ml_errors.push(new VError('need at least one ' +
		    'metadata server and one storage server'));
		return (layout);
	}

	if (dcconfig.dc_az_names.length != 1 &&
	    dcconfig.dc_az_names.length != 3) {
		layout.ml_errors.push(new VError('only one- and three-' +
		    'datacenter deployments are supported'));
		return (layout);
	}

	extrametadata = null;
	extrastorage = null;
	jsprim.forEachKey(dcconfig.dc_azs, function (azname, az) {
		if (az.rsaz_nmetadata !== dcconfig.dc_min_nmetadata_perdc) {
			extrametadata = azname;
		}

		if (az.rsaz_nstorage !== dcconfig.dc_min_nstorage_perdc) {
			extrastorage = azname;
		}
	});

	if (extrametadata !== null) {
		layout.ml_warnings.push(new VError('datacenters have ' +
		    'different numbers of metadata servers.  The impact of a ' +
		    'datacenter failure will differ depending on which ' +
		    'datacenter fails.'));
	}

	if (extrastorage !== null) {
		layout.ml_warnings.push(new VError('datacenters have ' +
		    'different numbers of storage servers.'));
	}

	if (dcconfig.dc_nshards > dcconfig.dc_min_nmetadata_perdc) {
		/*
		 * It doesn't make much sense to have more shards than metadata
		 * servers.  If you know at least two Manatee primaries will
		 * always be running on one host, what's the point of separating
		 * those into two shards?  It would usually make sense to just
		 * use fewer shards and expect the same performance.  Still,
		 * there can be exceptions, as for test environments, or
		 * environments that one expects to expand with new hardware
		 * (and where one would prefer to avoid resharding).  As a
		 * result, this is not a fatal error.
		 */
		layout.ml_warnings.push(new VError(
		    'requested %d shards with only %d metadata server%s in ' +
		    'at least one datacenter.  ' +
		    'Multiple primary databases will wind up running on the ' +
		    'same servers, and this configuration may not survive ' +
		    'server failure.  This is not recommended.',
		    dcconfig.dc_nshards, dcconfig.dc_min_nmetadata_perdc,
		    dcconfig.dc_min_nmetadata_perdc == 1 ? '' : 's'));
	} else if (ML_NPERSHARD_INSTANCES * dcconfig.dc_nshards >
	    dcconfig.dc_az_names.length * dcconfig.dc_min_nmetadata_perdc) {
		/*
		 * Strictly speaking, this case is just as bad as the previous
		 * one because you can wind up in the same state, with multiple
		 * primaries on the same server.  However, some operators may
		 * feel that it's a little better because they're not guaranteed
		 * to always be running in that degraded state.  The warning
		 * message is a little softer, but basically the same.
		 */
		layout.ml_warnings.push(new VError(
		    'requested %d shards with only %d metadata server%s in ' +
		    'at least one datacenter.  Under some conditions, ' +
		    'multiple databases may wind up ' +
		    'running on the same servers.  This is not recommended.',
		    dcconfig.dc_nshards, dcconfig.dc_min_nmetadata_perdc,
		    dcconfig.dc_min_nmetadata_perdc == 1 ? '' : 's'));
	}

	if (dcconfig.dc_rack_names.length < ML_NPERSHARD_INSTANCES) {
		layout.ml_warnings.push(new VError(
		    'configuration has only %d rack%s.  This configuration ' +
		    'may not survive rack failure.',
		    dcconfig.dc_rack_names.length,
		    dcconfig.dc_rack_names.length == 1 ? '' : 's'));
	}

	jsprim.forEachKey(images, function (svcname, image) {
		var count, alloc_class, cnid, i, j;

		if (ML_SERVICES_EXACT.hasOwnProperty(svcname) ||
		    ML_FRONTDOOR_RATIOS.hasOwnProperty(svcname)) {
			assertplus.ok(!services.serviceIsSharded(svcname));

			if (ML_SERVICES_EXACT.hasOwnProperty(svcname)) {
				alloc_class = 'small';
				count = ML_SERVICES_EXACT[svcname];
				assertplus.number(count);
			} else {
				/*
				 * We allocate all frontdoor services from the
				 * same class to avoid overweighting the first
				 * servers in each rack.  This way, the count of
				 * all front door services on each server cannot
				 * differ by more than one across all servers.
				 */
				alloc_class = 'frontdoor';

				/*
				 * This calculation means that whichever
				 * frontdoor service has the highest ratio gets
				 * one instance per metadata server.  The rest
				 * are scaled down proportionally.
				 */
				count = Math.ceil(
				    ML_FRONTDOOR_RATIOS[svcname] *
				    (dcconfig.dc_servers_metadata.length /
				    ML_FRONTDOOR_NMAXINSTANCES));
				assertplus.ok(count > 0);
				assertplus.ok(count <=
				    dcconfig.dc_servers_metadata.length);

				/*
				 * For availability, there should be at least
				 * two of each frontdoor service.
				 */
				count = Math.max(2, count);
			}

			for (i = 0; i < count; i++) {
				cnid = layout.allocateMetadataCn(alloc_class);
				layout.allocateInstance(cnid, svcname,
				    { 'IMAGE': image });
			}
		} else if (ML_SERVICES_PER_SHARD.hasOwnProperty(svcname)) {
			assertplus.ok(services.serviceIsSharded(svcname));
			for (i = 0; i < dcconfig.dc_nshards; i++) {
				for (j = 0; j < ML_NPERSHARD_INSTANCES; j++) {
					/*
					 * Per-shard services are allocated in
					 * their own allocation class so that
					 * different services are laid out the
					 * same way across the fleet (e.g.,
					 * moray instance "i" will be on the
					 * same CN as postgres instance "i").
					 * This relies on the deterministic
					 * nature of allocation to make sure
					 * that moray and postgres instances of
					 * the same shard are colocated.
					 */
					cnid = layout.allocateMetadataCn(
					    svcname);
					layout.allocateInstance(cnid, svcname, {
					    'SH': i + 1,
					    'IMAGE': image
					});
				}
			}
		} else if (svcname == 'storage') {
			dcconfig.dc_servers_storage.forEach(function (ocnid) {
				layout.allocateInstance(ocnid, svcname,
				    { 'IMAGE': image });
			});
		} else {
			assertplus.equal(svcname, 'marlin');
			dcconfig.dc_servers_storage.forEach(function (ocnid) {
				var server, avail_mb;

				server = dcconfig.dc_servers[ocnid];
				avail_mb = server.rscn_dram * 1024;
				avail_mb = ML_COMPUTE_DRAM_PERCENT * avail_mb;
				count = Math.floor(avail_mb /
				    ML_COMPUTE_DRAM_DEFAULT);
				count = Math.max(count, ML_COMPUTE_NMIN);

				for (i = 0; i < count; i++) {
					layout.allocateInstance(ocnid, svcname,
					    { 'IMAGE': image });
				}
			});
		}
	});

	return (layout);
}


/*
 * DcConfigLoader is a helper class for loading a configuration from either a
 * file or a raw object.  This takes care of parsing and validating the
 * configuration.
 *
 * After instantiation, you should call either loadDirectly() or loadFromFile().
 */
function DcConfigLoader()
{
	/* DcConfig we're going to load into */
	this.dcl_dcconfig = new DatacenterConfig();
	/* Human-readable source of the datacenter configuration. */
	this.dcl_source = null;

	/* Input stream for datacenter configuration */
	this.dcl_input = null;
	/* Accumulated data for datacenter configuration */
	this.dcl_data = null;
	/* Parsed representation of datacenter configuration */
	this.dcl_parsed = null;
	/* Errors accumulated during processing */
	this.dcl_errors = [];
	/* We've finished the loading process */
	this.dcl_done = false;
	/* Callback to invoke upon completion */
	this.dcl_callback = null;
}

/*
 * Given an object representing the datacenter configuration, populate the
 * dcconfig.  Arguments:
 *
 *     config	(object)   represents the datacenter configuration, in the same
 *			   format as the file-based interface expects after
 *			   parsing the file as a JSON object
 *
 * Invokes callback() upon completion, possibly with an error identifying
 * problems with the configuration.  If there's no error, then the datacenter
 * configuration is passed as the second argument.
 */
DcConfigLoader.prototype.loadDirectly = function (args, callback)
{
	assertplus.object(args, 'args');
	assertplus.object(args.config, 'args.config');
	assertplus.func(callback, 'callback');
	assertplus.ok(this.dcl_callback === null,
	    'cannot re-use DcConfigLoader');

	this.dcl_source = 'directly-passed';
	this.dcl_input = null;
	this.dcl_data = null;
	this.dcl_errors = [];
	this.dcl_callback = callback;
	this.dcl_parsed = jsprim.deepCopy(args.config);
	this.parse();
};

/*
 * Given a filename, populates the region dcconfig based on the contents of the
 * file.  Arguments:
 *
 *     filename	 (string)  path to configuration file
 *
 * Invokes callback() upon completion, possibly with an error identifying
 * problems with the configuration.  If there's no error, then the datacenter
 * configuration is passed as the second argument.
 */
DcConfigLoader.prototype.loadFromFile = function (args, callback)
{
	var self = this;

	assertplus.object(args, 'args');
	assertplus.string(args.filename, 'args.filename');
	assertplus.func(callback, 'callback');
	assertplus.ok(this.dcl_callback === null,
	    'cannot re-use DcConfigLoader');

	this.dcl_source = 'file: ' + JSON.stringify(args.filename);
	this.dcl_input = fs.createReadStream(args.filename);
	this.dcl_data = '';
	this.dcl_errors = [];
	this.dcl_callback = callback;

	this.dcl_input.on('error', function (err) {
		self.dcl_errors.push(new VError(err, self.dcl_source));
		self.finish();
	});

	this.dcl_input.on('data', function (chunk) {
		self.dcl_data += chunk.toString('utf8');
	});

	this.dcl_input.on('end', function () {
		self.parseFromJson();
	});
};

/*
 * Invoked when we've finished reading the input file and are ready to parse it.
 * This parses the JSON, validates it, and then loads it into the
 * DatacenterConfig object.
 */
DcConfigLoader.prototype.parseFromJson = function ()
{
	if (this.dcl_errors.length === 0) {
		assertplus.string(this.dcl_data);
		try {
			this.dcl_parsed = JSON.parse(this.dcl_data);
		} catch (ex) {
			this.dcl_errors.push(
			    new VError(ex, 'parse %s', this.dcl_source));
		}
	}

	this.parse();
};

DcConfigLoader.prototype.parse = function ()
{
	var dcconfig, err, svcname;
	var racknames;
	var self = this;

	if (this.dcl_errors.length === 0) {
		err = jsprim.validateJsonObject(ML_SCHEMA, this.dcl_parsed);
		if (err instanceof Error) {
			this.dcl_errors.push(err);
		}

		if (this.dcl_parsed.hasOwnProperty('images')) {
			for (svcname in this.dcl_parsed['images']) {
				if (typeof (this.dcl_parsed['images'][svcname])
				    != 'string') {
					this.dcl_errors.push(new VError(
					    'images[%s]: not a string',
					    svcname));
					break;
				}

				if (!services.serviceNameIsValid(svcname)) {
					this.dcl_errors.push(new VError(
					    'images[%s]: invalid service name',
					    svcname));
					break;
				}
			}
		}
	}

	if (this.dcl_errors.length > 0) {
		/*
		 * At this point, we can only have seen one error: either a
		 * failure to open or read the file or a failure to parse the
		 * contents.
		 */
		assertplus.equal(1, this.dcl_errors.length);
		this.finish();
		return;
	}

	dcconfig = this.dcl_dcconfig;
	dcconfig.dc_images = jsprim.deepCopy(this.dcl_parsed['images']);

	/* This should be validated by the JSON schema. */
	assertplus.number(this.dcl_parsed['nshards']);
	dcconfig.dc_nshards = this.dcl_parsed['nshards'];
	assertplus.arrayOfObject(this.dcl_parsed['servers']);
	this.dcl_parsed['servers'].forEach(function (server) {
		var type, cn, rackname, rack, az;

		assertplus.ok(server !== null);
		assertplus.string(server['type']);
		type = server['type'];
		assertplus.string(server['uuid']);
		cn = server['uuid'];
		assertplus.number(server['memory']);

		assertplus.optionalString(server['rack']);
		if (server.hasOwnProperty('rack')) {
			rackname = server['rack'];
		} else {
			rackname = ML_DEFAULT_RACK;
		}

		assertplus.optionalString(server['az']);
		if (server.hasOwnProperty('az')) {
			az = server['az'];
		} else {
			az = ML_DEFAULT_AZ;
		}

		if (!dcconfig.dc_azs.hasOwnProperty(az)) {
			dcconfig.dc_az_names.push(az);
			dcconfig.dc_azs[az] = {
			    'rsaz_name': az,
			    'rsaz_rack_names': [],
			    'rsaz_nstorage': 0,
			    'rsaz_nmetadata': 0
			};
		}

		if (!dcconfig.dc_racks.hasOwnProperty(rackname)) {
			dcconfig.dc_rack_names.push(rackname);
			dcconfig.dc_azs[az].rsaz_rack_names.push(rackname);
			rack = dcconfig.dc_racks[rackname] = {
			    'rsrack_az': az,
			    'rsrack_name': rackname,
			    'rsrack_servers_metadata': [],
			    'rsrack_servers_storage': []
			};
		} else {
			rack = dcconfig.dc_racks[rackname];
			if (rack.rsrack_az != az) {
				self.dcl_errors.push(new VError(
				    'server %s, rack %s, az %s: rack already ' +
				    'exists in different az %s', cn, rackname,
				    az, rack.rsrack_az));
			}
		}

		if (dcconfig.dc_servers.hasOwnProperty(cn)) {
			self.dcl_errors.push(new VError(
			    'server %s, rack %s, az %s: duplicate server',
			    cn, rackname, az));
		}

		dcconfig.dc_server_names.push(cn);
		dcconfig.dc_servers[cn] = {
		    'rscn_uuid': cn,
		    'rscn_rack': rackname,
		    'rscn_dram': server['memory']
		};

		if (type == 'metadata') {
			rack.rsrack_servers_metadata.push(cn);
			dcconfig.dc_servers_metadata.push(cn);
			dcconfig.dc_azs[az].rsaz_nmetadata++;
		} else {
			assertplus.equal(type, 'storage');
			rack.rsrack_servers_storage.push(cn);
			dcconfig.dc_servers_storage.push(cn);
			dcconfig.dc_azs[az].rsaz_nstorage++;
		}
	});

	if (this.dcl_errors.length > 0) {
		this.finish();
		return;
	}

	jsprim.forEachKey(dcconfig.dc_azs, function (_, az) {
		if (dcconfig.dc_min_nmetadata_perdc === null ||
		    dcconfig.dc_min_nmetadata_perdc > az.rsaz_nmetadata) {
			dcconfig.dc_min_nmetadata_perdc = az.rsaz_nmetadata;
		}

		if (dcconfig.dc_min_nstorage_perdc === null ||
		    dcconfig.dc_min_nstorage_perdc > az.rsaz_nstorage) {
			dcconfig.dc_min_nstorage_perdc = az.rsaz_nstorage;
		}
	});

	assertplus.number(dcconfig.dc_min_nmetadata_perdc);
	assertplus.number(dcconfig.dc_min_nstorage_perdc);

	/*
	 * Construct the list of rack names by sorting the rack names within
	 * each AZ and then selecting racks from each AZ's list.  By doing this,
	 * spreading zones across racks is enough to also spread them across
	 * AZs.
	 */
	jsprim.forEachKey(dcconfig.dc_azs, function (_, az) {
		az.rsaz_rack_names.sort();
	});
	racknames = common.stripe.call(null,
	    dcconfig.dc_az_names.map(function (azname) {
		return (dcconfig.dc_azs[azname].rsaz_rack_names);
	    }));
	assertplus.deepEqual(racknames.slice(0).sort(),
	    dcconfig.dc_rack_names.sort());
	dcconfig.dc_rack_names = racknames;

	assertplus.deepEqual(Object.keys(dcconfig.dc_azs).sort(),
	    dcconfig.dc_az_names.slice(0).sort());
	assertplus.deepEqual(Object.keys(dcconfig.dc_racks).sort(),
	    dcconfig.dc_rack_names.slice(0).sort());
	assertplus.deepEqual(Object.keys(dcconfig.dc_servers).sort(),
	    dcconfig.dc_server_names.slice(0).sort());

	/* Validated by the JSON schema. */
	assertplus.ok(dcconfig.dc_server_names.length > 0);
	this.finish();
};

/*
 * Invoked exactly once for each instance when loading is complete, either as a
 * result of an error or normal completion.
 */
DcConfigLoader.prototype.finish = function ()
{
	assertplus.ok(!this.dcl_done);
	this.dcl_done = true;

	if (this.dcl_errors.length > 0) {
		this.dcl_callback(this.dcl_errors[0]);
	} else {
		this.dcl_callback(null, this.dcl_dcconfig);
	}
};


/*
 * The DatacenterConfig is a plain-old-JavaScript-object that represents the set
 * of datacenters, racks, and servers that we have available for deploying
 * Manta, along with configuration properties that control the deployment (like
 * the total number of shards).  This object must be initialized using the
 * DcConfigLoader object.  After that, it's immutable.
 */
function DatacenterConfig()
{
	/*
	 * Availability zone information.  Each availability zone object has
	 * properties:
	 *
	 *    rsaz_name       (string) name of this availability zone
	 *    rsaz_rack_names (array)  list of rack identifiers in this AZ
	 *    rsaz_nmetadata  (num)    count of metadata servers in this AZ
	 *    rsaz_nstorage   (num)    count of storage servers in this AZ
	 */
	/* list of az names in this region */
	this.dc_az_names = [];
	/* mapping of az names to availability zone objects (see above). */
	this.dc_azs = {};

	/*
	 * Rack information.  Rack names are assumed to be unique across all
	 * datacenters.  Each rack object has properties:
	 *
	 *     rsrack_name    		(string) name of this rack
	 *     rsrack_az      		(string) availability zone where this
	 *					 rack lives
	 *     rsrack_servers_metadata  (array)  list of uuids for metadata
	 *					 servers in this rack
	 *     rsrack_servers_storage   (array)  list of uuids for storage
	 *					 servers in this rack
	 */
	/* list of rack names in this region */
	this.dc_rack_names = [];
	/* mapping of rack names to rack objects (see above) */
	this.dc_racks = {};

	/*
	 * Server information.  Server names are assumed to be unique across all
	 * datacenters.  Each server object has properties:
	 *
	 *     rscn_uuid (string) unique identifier for this server
	 *     rscn_rack (string) name of the rack where this server lives
	 *     rscn_dram (number) gigabytes of memory available for Manta
	 */
	/* list of server names (uuids) */
	this.dc_server_names = [];
	/* mapping of server names to server objects (see above) */
	this.dc_servers = {};
	/* list of metadata server names */
	this.dc_servers_metadata = [];
	/* list of storage server names */
	this.dc_servers_storage = [];

	/* minimum count of metadata servers across all DCs */
	this.dc_min_nmetadata_perdc = null;
	/* minimum count of storage servers across all DCs */
	this.dc_min_nstorage_perdc = null;

	/* Number of metadata shards */
	this.dc_nshards = null;
	/* Image overrides */
	this.dc_images = null;
}


/*
 * A Layout object completely specifies how many instances of which versions of
 * all services should be deployed on each server within a region.  Private
 * interfaces are provided here to build up the layout, but once it's
 * constructed, it's immutable.  The only methods exposed to other files in this
 * module are azs(), serialize(), printIssues(), printSummary(), and nerrors().
 *
 * This is effectively a programmatic representation of the "manta-adm update"
 * data structure.  Some code is shared between them, but more functionality
 * could be commonized.
 */
function Layout(dcconfig)
{
	var self = this;

	/* reference to dc config containing detailed metadata */
	this.ml_dcconfig = dcconfig;

	/* non-fatal problems with this layout */
	this.ml_warnings = [];

	/* fatal problems with this layout */
	this.ml_errors = [];

	/*
	 * mapping server uuid -> service name -> ServiceConfiguration
	 *
	 * Describes the instances deployed on each server.  This is the final
	 * output of this process.
	 */
	this.ml_configs_byserver = {};

	/*
	 * mapping of svcname -> az -> ServiceConfiguration
	 *
	 * Describes the instances deployed in each AZ.
	 */
	this.ml_configs_bysvcname_az = {};

	/*
	 * mapping of service name -> ServiceConfiguration
	 *
	 * Describes the instances deployed in the whole region.
	 */
	this.ml_configs_bysvcname = {};

	/*
	 * Mutable state used for allocation.
	 *
	 * For metadata services, we construct a list of servers up front that
	 * stripes across racks, which are already sorted so that they stripe
	 * across AZs.  Then we just allocate from this list in order, cycling
	 * back when we run out.  See the comment in allocateMetadataCn().
	 */
	this.ml_metadata_i = {};
	this.ml_metadata_striped = common.stripe(
	    this.ml_dcconfig.dc_rack_names.map(function (rackname) {
		return (self.ml_dcconfig.dc_racks[
		    rackname].rsrack_servers_metadata);
	    }));
}

/*
 * Returns the uuid of the server that should be used for the next allocation of
 * class "alloc_class".  Internal state is modified so that subsequent server
 * allocations take this allocation into account, but this method does not
 * actually assign an instance to this server.  You have to call
 * allocateInstance() with the returned server uuid in order to do that.
 *
 * This allocator stripes services across all of the metadata servers,
 * preferring to use servers in different racks when possible.  This process is
 * deterministic.  Allocations look like this:
 *
 *     rack 0, server 0
 *     rack 1, server 0
 *     rack 2, server 0
 *     ...
 *     rack 0, server 1
 *     rack 1, server 1
 *     rack 2, server 1
 *     ...
 *     rack 0, server 2
 *     ...
 *
 * If some racks have fewer servers than others, those racks will wind up with
 * fewer total instances.  This likely means that per-rack network utilization
 * and the percentage of total capacity lost after a rack failure will vary
 * across racks.
 *
 * The "alloc_class" is an arbitrary string token used to support orthogonal
 * groups of allocations.  The above describes what happens for sequential
 * allocations from the same "alloc_class".  Allocations for a new "alloc_class"
 * start over at rack 0, server 0, and these can be mixed with allocations for
 * other values of "alloc_class".
 */
Layout.prototype.allocateMetadataCn = function (alloc_class)
{
	var which;

	if (!this.ml_metadata_i.hasOwnProperty(alloc_class)) {
		this.ml_metadata_i[alloc_class] = 0;
	}

	which = (this.ml_metadata_i[alloc_class]++) %
	    this.ml_metadata_striped.length;
	return (this.ml_metadata_striped[which]);
};

Layout.prototype.cnidToAzName = function (cnid)
{
	var server, rack;
	server = this.ml_dcconfig.dc_servers[cnid];
	assertplus.ok(server);
	rack = this.ml_dcconfig.dc_racks[server.rscn_rack];
	assertplus.ok(rack);
	return (rack.rsrack_az);
};

/*
 * Assign one instance of service "svcname" having config "config" to compute
 * node "cnid".  "config" describes the image (and possibly shard) for this
 * instance.  See the ServiceConfiguration class for details.
 */
Layout.prototype.allocateInstance = function (cnid, svcname, config)
{
	var azname;

	assertplus.string(cnid);
	assertplus.string(svcname);

	if (!this.ml_configs_byserver.hasOwnProperty(cnid)) {
		this.ml_configs_byserver[cnid] = {};
	}
	if (!this.ml_configs_byserver[cnid].hasOwnProperty(svcname)) {
		this.ml_configs_byserver[cnid][svcname] =
		    new services.ServiceConfiguration(
		    services.serviceConfigProperties(svcname));
	}

	this.ml_configs_byserver[cnid][svcname].incr(config);

	if (!this.ml_configs_bysvcname.hasOwnProperty(svcname)) {
		this.ml_configs_bysvcname[svcname] =
		    new services.ServiceConfiguration(
		    services.serviceConfigProperties(svcname));
		this.ml_configs_bysvcname_az[svcname] = {};
	}

	this.ml_configs_bysvcname[svcname].incr(config);

	azname = this.cnidToAzName(cnid);
	if (!this.ml_configs_bysvcname_az[svcname].hasOwnProperty(azname)) {
		this.ml_configs_bysvcname_az[svcname][azname] =
		    new services.ServiceConfiguration(
		    services.serviceConfigProperties(svcname));
	}

	this.ml_configs_bysvcname_az[svcname][azname].incr(config);
};

/*
 * Returns the list of datacenter names laid out in this configuration.
 */
Layout.prototype.azs = function ()
{
	return (this.ml_dcconfig.dc_az_names.slice(0));
};

/*
 * Prints to "errstream" a list of problems encountered generating this
 * configuration.
 */
Layout.prototype.printIssues = function (errstream)
{
	if (this.ml_errors.length > 0) {
		this.ml_errors.forEach(function (err) {
			errstream.write('error: ' + err.message + '\n');
		});
	} else if (this.ml_warnings.length > 0) {
		this.ml_warnings.forEach(function (err) {
			errstream.write('warning: ' + err.message + '\n');
		});
	}
};

/*
 * Returns a JSON description of the generated layout for datacenter "azname".
 * This description is suitable for use by "manta-adm update".
 */
Layout.prototype.serialize = function (azname)
{
	var config, cnids;
	var self = this;

	if (this.nerrors() > 0) {
		return (null);
	}

	/*
	 * For human verification, it's most convenient if the servers are
	 * grouped by type of node and the services are presented in order.
	 */
	config = {};
	cnids = this.ml_dcconfig.dc_servers_metadata.concat(
	    this.ml_dcconfig.dc_servers_storage);
	cnids.forEach(function (cnid) {
		var cfgs, svcnames;

		if (self.cnidToAzName(cnid) != azname) {
			return;
		}

		config[cnid] = {};
		assertplus.ok(self.ml_configs_byserver.hasOwnProperty(cnid));
		cfgs = self.ml_configs_byserver[cnid];
		svcnames = Object.keys(cfgs).sort();
		svcnames.forEach(function (svcname) {
			var svccfg = cfgs[svcname];
			config[cnid][svcname] = svccfg.summary();
		});
	});

	return (JSON.stringify(config, null, '    ') + '\n');
};

/*
 * Returns the number of fatal errors associated with this configuration.
 */
Layout.prototype.nerrors = function ()
{
	return (this.ml_errors.length);
};

/*
 * Prints to "outstream" a summary of the configuration for sanity-checking.
 */
Layout.prototype.printSummary = function (outstream)
{
	var columns, out, cfgs;
	var svcnames;

	if (this.nerrors() > 0) {
		return (null);
	}

	columns = [ {
	    'label': '',
	    'width': 4
	}, {
	    'label': 'SERVICE',
	    'width': 16
	}, {
	    'label': 'SHARD',
	    'align': 'right',
	    'width': 5
	} ];

	this.azs().forEach(function (azname) {
		columns.push({
		    'label': azname,
		    'align': 'right',
		    'width': 16
		});
	});

	out = new tab.TableOutputStream({
	    'stream': outstream,
	    'columns': columns
	});

	/*
	 * We'll print the services in this table in the order they normally
	 * appear in "manta-adm" output, except that we'll put the sharded ones
	 * last.
	 *
	 * Each non-sharded service has one row in the table showing counts of
	 * all instances of that service in each AZ.
	 */
	cfgs = this.ml_configs_bysvcname_az;
	svcnames = services.mSvcNames.filter(
	    function (a) { return (!services.serviceIsSharded(a)); });
	svcnames.forEach(function (svcname) {
		var row;

		row = {};
		row['SERVICE'] = svcname;
		row['SHARD'] = '-';
		jsprim.forEachKey(cfgs[svcname], function (azname, svccfg) {
			svccfg.each(function (cfg) {
				/*
				 * There should be only one config per service
				 * per AZ because we would have used the same
				 * image for all services.
				 */
				assertplus.ok(!row.hasOwnProperty(azname));
				assertplus.number(cfg['count']);
				row[azname] = cfg['count'];
			});
		});

		out.writeRow(row);
	});

	/*
	 * Sharded services have one row per shard showing the counts of
	 * instances of that service for that shard in each AZ.
	 */
	svcnames = services.mSvcNames.filter(services.serviceIsSharded);
	svcnames.forEach(function (svcname) {
		/*
		 * The only way we have to iterate shards is within a single AZ,
		 * so we collect information in "rowsbyshard" first and then
		 * emit all of the rows when we're done.
		 */
		var rowsbyshard = {};
		jsprim.forEachKey(cfgs[svcname], function (azname, svccfg) {
			svccfg.each(function (cfg) {
				var shard, shardrow;

				shard = cfg['SH'];
				assertplus.number(shard);
				if (rowsbyshard.hasOwnProperty(shard)) {
					shardrow = rowsbyshard[shard];
					/*
					 * Like the case above, we should not
					 * have already seen this
					 * service/AZ/shard combination because
					 * that would imply that we were using
					 * more than one image for this service.
					 */
					assertplus.ok(!shardrow.hasOwnProperty(
					    azname));
				} else {
					shardrow = {};
					shardrow['SERVICE'] = svcname;
					shardrow['SHARD'] = shard;
					rowsbyshard[shard] = shardrow;
				}

				shardrow[azname] = cfg['count'];
			});
		});

		/*
		 * Shards are identified by numbers stored as strings.  To sort
		 * them the way a person would like, we need to parse them as
		 * integers and compare those.
		 */
		Object.keys(rowsbyshard).sort(function (a, b) {
			return (parseInt(a, 10) - parseInt(b, 10));
		}).forEach(function (shard) {
			out.writeRow(rowsbyshard[shard]);
		});
	});
};
