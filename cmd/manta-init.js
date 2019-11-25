#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * manta-init.js: initialize manta in a datacenter
 */

var assert = require('assert-plus');
var async = require('async');
var child_process = require('child_process');
var common = require('../lib/common');
var https = require('https');
var fs = require('fs');
var optimist = require('optimist');
var path = require('path');
var sdc = require('sdc-clients');
var services = require('../lib/services');
var ssh = require('../lib/ssh');
var url = require('url');
var node_uuid = require('node-uuid');
var vasync = require('vasync');
var verror = require('verror');

var Logger = require('bunyan');

var exec = require('child_process').exec;
var sprintf = require('util').format;


var VError = verror.VError;

// -- Globals

var POSEIDON;
var POSEIDON_LOGIN = 'poseidon';
var POSEIDON_PASSWORD = 'trident123';

/*
 * If the -c option isn't specified, the default is to download 10 images in
 * parallel.
 */
var CONCURRENCY = 10;

optimist.usage('Usage:\tmanta-init -e <email>');

var ARGV = optimist.options({
	'B': {
		alias: 'branch',
		describe: 'the branch substring to use when looking for images '
			+ '(default: no filter)',
		default: ''
	},
	'C': {
		alias: 'channel',
		describe: 'the channel to use'
	},
	'c': {
		alias: 'concurrent_downloads',
		describe: 'number of concurrent image downloads (default: 10)'
	},
	'e': {
		alias: 'email',
		describe: 'operator email for alarms',
		demand: true
	},
	'l': {
		alias: 'log_file',
		describe: 'dump logs to this file (stdout to dump to console)',
		'default': '/var/log/manta-init.log'
	},
	'm': {
		alias: 'marlin_image',
		describe: 'Use the specified image_uuid instead of '
			+ 'searching for latest'
	},
	'n': {
		alias: 'no_download',
		describe: 'omit downloading new images'
	},
	's': {
		alias: 'size',
		describe: 'deployment size (i.e. coal, lab, production)',
		demand: false
	}
}).argv;

function usage(message) {
	if (message) {
		console.error(message);
	}
	optimist.showHelp();
	process.exit(2);
}

// -- User management

function addUser(user, cb) {
	var ufds = self.UFDS;
	var log = self.log;

	assert.object(user, 'user');
	assert.string(user.login, 'user.login');
	assert.string(user.userpassword, 'user.userpassword');
	assert.string(user.email, 'user.email');

	log.info('creating %s', user.login);

	ufds.addUser(user, function (err, ret) {
		if (err) {
			log.error(err, 'failed to add %s', user.login);
			return (cb(err));
		}

		log.info('created %s', user.login);
		return (cb(null, ret));
	});
}

function getOrCreateUser(user, cb) {
	var ufds = self.UFDS;
	var log = self.log;

	assert.object(user, 'user');
	assert.string(user.login, 'user.login');
	assert.string(user.userpassword, 'user.userpassword');

	log.info('getting or creating user %s', user.login);

	ufds.getUser(user.login, function (err, ret) {
		if (err && err.statusCode !== 404) {
			log.error(err, 'failed to get %s', user.login);
			return (cb(err));
		} else if (err) {
			assert.ok(err.statusCode === 404);
			return (addUser(user, cb));
		}

		return (cb(null, ret));
	});
}

function updateEmail(user, email, cb) {
	var ufds = self.UFDS;
	var log = self.log;

	assert.object(user, 'user');
	assert.string(user.login, 'user.login');
	assert.string(user.email, 'user.email');

	if (user.email === email) {
		log.info('email for user %s is %s; not updating UFDS',
		    user.login, user.email);
		return (cb(null));
	}

	var changes = {};
	changes.email = email;

	ufds.updateUser(user.login, changes, function (err) {
		if (err) {
			log.error(err, 'failed to update email');
			return (cb(err));
		}

		log.info({ email: email }, 'updated %s\'s email',
		    user.login);

		return (cb(null));
	});

	return (null);
}


// -- Network management

/*
 * Update the specified network, network_pool, and any networks that are part
 * of the named network_pool to include owner_uuid in the set of users allowed
 * to provision zones on this network.
 */

/*
 * Callback method for common.updateNetworkUsers.
 */
function addUserToNetwork(owner_uuid, network_owners, callback) {
	var uuids = [];

	if (network_owners.indexOf(owner_uuid) !== -1) {
		var e = new VError({
		    name: 'UserExistsError',
		    info: { errno: 'EEXISTS' }
		}, 'user %s already allowed for this network', owner_uuid);
		callback(e);
		return;
	}

	uuids = network_owners.concat(owner_uuid);

	callback(null, uuids);
}


// -- Image management

function sortByDate(images) {
	var sorted = images.sort(function (item1, item2) {
		var date1 = new Date(item1.published_at);
		var date2 = new Date(item2.published_at);
		return (date2 - date1);
	});

	return (sorted[0]);
}

function findLatestImage(service, cb) {
	var remote_imgapi = self.REMOTE_IMGAPI;
	var log = self.log;

	var image_name = services.serviceNameToImageName(service);
	var version_substr = ARGV.branch;
	// This is usually the channel set in SAPI if no -C argument was
	// passed. If there's no channel in SAPI, we use the server's
	// default channel.
	var channel = ARGV.channel;

	if (channel === null) {
		log.info('finding image %s for service %s on ' +
		    'default update channel', image_name, service);
	} else {
		log.info('finding image %s for service %s on channel "%s"',
		    image_name, service, channel);
	}

	var onSearchFinish = function (err, image) {
		if (err) {
			log.error(err);
			return (cb(err));
		}
		if (image === undefined) {
			var msg = sprintf(
			    'Unable to find image %s for %s on channel "%s"',
			    image_name, service, channel);
			log.error(msg);
			return (cb(new Error(msg)));
		}
		log.info({ image: image }, 'found image %s for %s',
		    image_name, service);

		return (cb(null, image));
	};

	/*
	 * If -n is used, find the most recent image which is installed in
	 * this datacenter's IMGAPI, assuming it matches our version_substr.
	 */
	if (ARGV.n) {
		return (findLatestLocalImage(
		    image_name, version_substr, onSearchFinish));
	}

	var filters = {};
	filters.name = image_name;
	if (version_substr.length > 0) {
		log.info('search restricted to version substring: %s',
		    version_substr);
		filters.version = '~' + version_substr;
	}

	if (channel) {
		filters.channel = channel;
	}

	log.info({ filters: filters }, 'search for images');

	remote_imgapi.listImages(filters, function (err, images) {
		if (err) {
			log.error(err, 'failed to search for images with ' +
				'name like %s', image_name);
			return (cb(err));
		}

		onSearchFinish(null, sortByDate(images));
	});
}

function findLatestLocalImage(image_name, version_substr, cb) {
	var imgapi = self.IMGAPI;
	var log = self.log;

	log.info('search for image %s restricted to local images', image_name);

	var filters = {};
	filters.name = image_name;
	if (version_substr.length > 0) {
		log.info('search restricted to version substring: %s',
		    version_substr);
		filters.version = '~' + version_substr;
	}

	imgapi.listImages(filters, function (err, images) {
		if (err) {
			log.error(err, 'failed to list local images');
			return (cb(err));
		}

		if (images.length === 0) {
			var suberr = new Error(
			    sprintf('no local image found for %s (remove -n?)',
			    image_name));
			return (cb(suberr));
		}

		return (cb(null, sortByDate(images)));
	});
}

function updateServiceImage(svc, image_uuid, cb) {
	var sapi = self.SAPI;
	var log = self.log;

	assert.object(svc, 'svc');
	assert.object(svc.params, 'svc.params');
	assert.string(image_uuid, 'image_uuid');
	assert.func(cb, 'cb');

	if (svc.params.image_uuid === image_uuid) {
		log.info('service %s already has image %s',
		    svc.uuid, image_uuid);
		return (cb(null));
	}

	var changes = {};
	changes.params = {};
	changes.params.image_uuid = image_uuid;

	log.info('updating service %s from image %s to image %s',
	    svc.uuid, svc.params.image_uuid, image_uuid);

	sapi.updateService(svc.uuid, changes, function (err) {
		if (err) {
			log.error(err, 'failed to update service %s',
			    svc.uuid);
			return (cb(err));
		}

		log.info('updated service %s to image %s',
		    svc.uuid, image_uuid);
		return (cb(null));
	});
}


// -- SAPI methods

function addConfig(dirname, updatefunc, cb) {
	var sapi = self.SAPI;

	assert.string(dirname, 'dirname');
	assert.func(updatefunc, 'updatefunc');

	sapi.loadManifests(dirname, function (err, manifests) {
		if (err)
			return (cb(err));

		assert.object(manifests, 'manifests');

		var changes = {};
		changes.manifests = manifests;

		updatefunc(changes, cb);
	});
}

// -- Mainline

var self = this;

var bstreams = [ {
	level: 'debug',
	path: ARGV.l
} ];
if (ARGV.l === 'stdout') {
	bstreams = [ {
		level: 'debug',
		stream: process.stdout
	} ];
} else {
	console.error('logs at ' + ARGV.l);
}
self.log = new Logger({
	name: 'manta-init',
	serializers: Logger.stdSerializers,
	streams: bstreams
});

/*
 * node-optimist infers the type of an argument from the way it was used.  For
 * example:
 *
 *     -c foo        ARGV.c is the string "foo"
 *     -c 37         ARGV.c is the number 37
 *     -c ' 37'      ARGV.c is the number 37
 *     -c 37invalid  ARGV.c is the string "37invalid"
 *     -c            ARGV.c is the boolean true
 *     -c ''         ARGV.c is the boolean true
 *     -c ' '        ARGV.c is the number 0
 *     (-c left out) ARGV.c is undefined
 *
 * Note that this has the usual problem of attempting to coerce a string to a
 * number with Number: a string of all spaces is parsed as 0.  We should be
 * using jsprim.parseInteger() here, but for now, we only handle the two cases
 * that we intend to support.
 */
if (typeof (ARGV.c) == 'number' && ARGV.c > 0 && ARGV.c < 128 &&
    Math.floor(ARGV.c) == ARGV.c) {
	CONCURRENCY = ARGV.c;
} else if (ARGV.c !== undefined) {
	/*
	 * It would be nice to provide the user with value that we
	 * failed to parse, but optimist has potentially mangled it
	 * badly by the time we get here, so we don't bother.
	 */
	usage('unsupported value for "-c" option ' +
	    '(must be a positive integer less than 128)');
}

if (typeof (ARGV.channel) === 'boolean' || ARGV.channel === '*') {
	usage('unsupported value for "-C" option ' +
	    '(must not be "*" or an empty string)');
}

var pipelineFuncs = [
	function verifyArgs(_, cb) {
		if (ARGV.s &&
		    ['coal', 'lab', 'production'].indexOf(ARGV.s) === -1) {
			return (cb(new Error('size option must be one of,' +
					     ' coal, lab, or production.')));

		}
		return (cb(null));
	},

	function initClients(_, cb) {
		common.initSdcClients.call(self, cb);
	},

	function ensureFullMode(_, cb) {
		var sapi = self.SAPI;
		var log = self.log;

		sapi.getMode(function (err, mode) {
			if (err) {
				log.error(err, 'failed to get SAPI mode');
				return (cb(err));
			}

			if (mode === 'full') {
				log.info('SAPI in full mode');
				return (cb(null));
			}

			log.info('upgrading SAPI to full mode');

			sapi.setMode('full', function (suberr) {
				if (suberr) {
					log.error(suberr, 'failed to upgrade ' +
					    'to SAPI full mode');
					return (cb(suberr));
				}

				log.info('upgraded SAPI to full mode');

				return (cb(null));
			});
		});
	},

	function getOrCreatePoseidon(ctx, cb) {
		var user = {};
		user.login = POSEIDON_LOGIN;
		user.userpassword = POSEIDON_PASSWORD;
		if (ARGV.e)
			user.email = ARGV.e;

		getOrCreateUser(user, function gotUser(err, userRes) {
			if (err) {
				return (cb(err));
			}
			ctx.user = userRes;
			return (cb(null));
		});
	},

	function updatePoseidonEmail(ctx, cb) {
		var user = ctx.user;
		assert.object(user, 'user');
		assert.string(user.login, 'user.login');

		POSEIDON = user;

		if (!ARGV.e)
			return (cb(null));

		updateEmail(user, ARGV.e, function (err)  {
			POSEIDON.email = ARGV.e;
			return (cb(err));
		});

		return (null);
	},

	function addPoseidonToOperators(_, cb) {
		var ufds = self.UFDS;
		var log = self.log;

		assert.object(POSEIDON, 'POSEIDON');
		assert.string(POSEIDON.dn, 'POSEIDON.dn');

		var entry = {
			type: 'add',
			modification: {
				uniquemember: POSEIDON.dn
			}
		};

		var operatorsdn = 'cn=operators, ou=groups, o=smartdc';

		log.info({ entry: entry, operatorsdn: operatorsdn },
		    'adding poseidon to operators group');

		ufds.modify(operatorsdn, entry, function (err, res) {
			if (err) {
				log.error(err, 'failed to add poseidon to ' +
				    'operators group');
			}

			return (cb(err));
		});
	},

	function enableAdminProvisioning(_, cb) {
		var networks = [ 'manta', 'mantanat', 'admin' ];

		vasync.forEachParallel({
			func: function (network, subcb) {
				common.updateNetworkUsers({
				    name: network,
				    owner_uuid: POSEIDON.uuid,
				    napi: self.NAPI,
				    log: self.log,
				    action: 'add',
				    update_func: addUserToNetwork
				}, subcb);
			},
			inputs: networks
		}, function (err, results) {
			cb(err);
		});
	},

	function getMantaApplication(_, cb) {
		var log = self.log;
		log.info('fetching manta application from sapi');

		common.getMantaApplication.call(self,
		    POSEIDON.uuid, function (err, app) {
			if (app)
				self.manta_app = app;
			cb(err);
		});
	},

	function createMantaApplication(_, cb) {
		var log = self.log;
		var sapi = self.SAPI;

		log.info('building up manta application description');

		var file = path.join(path.dirname(__filename),
		    '../config/application.json');
		file = path.resolve(file);

		var extra = {
			metadata: {
				MANTAV: 2
			}
		};

		if (self.config.region_name === undefined ||
		    self.config.region_name === '') {
			return (cb(new Error('config file did not contain a ' +
					'region_name.')));
		}

		extra.metadata['REGION'] = self.config.region_name;
		extra.metadata['SIZE'] = ARGV.s || 'lab';

		extra.metadata['DNS_DOMAIN'] = self.config.dns_domain;
		extra.metadata['DOMAIN_NAME'] = sprintf('%s.%s',
		    extra.metadata['REGION'], extra.metadata['DNS_DOMAIN']);

		extra.metadata['MANTA_SERVICE'] = sprintf('manta.%s',
		    extra.metadata['DOMAIN_NAME']);
		extra.metadata['AUTH_SERVICE'] = sprintf('authcache.%s',
		    extra.metadata['DOMAIN_NAME']);
		extra.metadata['ELECTRIC_MORAY'] = sprintf('electric-moray.%s',
		    extra.metadata['DOMAIN_NAME']);
		extra.metadata['ELECTRIC_BORAY'] = sprintf('electric-boray.%s',
		    extra.metadata['DOMAIN_NAME']);
		extra.metadata['POSEIDON_UUID'] = POSEIDON.uuid;
		extra.metadata['IMGAPI_SERVICE'] =
		    url.format(self.IMGAPI.client.url);
		extra.metadata['WORKFLOW_SERVICE'] = sprintf('workflow.%s',
		    extra.metadata['DOMAIN_NAME']);
		extra.metadata['MEDUSA_REFLECTOR'] = sprintf('medusa.%s',
		    extra.metadata['DOMAIN_NAME']);

		extra.metadata['MANTA_URL'] = sprintf('https://%s',
		    extra.metadata['MANTA_SERVICE']);
		extra.metadata['MANTA_REJECT_UNAUTHORIZED'] = false;
		extra.metadata['MANTA_TLS_INSECURE'] = '1';
		extra.metadata['MUSKIE_MULTI_DC'] = false;
		extra.metadata['BUCKETS_API_MULTI_DC'] = false;

		/*
		 * Because of a series of unfortunate bugs, there was a flag
		 * day between mako (storage) and muskie (webapi) related to
		 * HTTP keepalives. It's unsafe to enable keepalives at the mako
		 * end until the muskies are sufficiently up to date. As a
		 * result, this SAPI metadata key has to be set to do so. For
		 * new deploys, we assume the muskie image used will be new
		 * enough and we can safely set it here.
		 *
		 * See also MANTA-3966, MANTA-3083, MANTA-3084
		 */
		extra.metadata['MAKO_HTTP_KEEPALIVE_TIMEOUT'] = 86400;

		// This is filled in when marlin is deployed.
		extra.metadata['SERVER_COMPUTE_ID_MAPPING'] = {};

		extra.master = true;
		extra.include_master = true;

		log.info('creating manta application');

		function onResponse(err, app) {
			if (err) {
				return (cb(err));
			}

			self.sapi_application = app;
			return (cb(null));
		}

		sapi.getOrCreateApplication('manta', POSEIDON.uuid, file,
					    extra, onResponse);
	},

	function addAdminKey(_, cb) {
		var log = self.log;
		var sapi = self.SAPI;
		var app = self.sapi_application;

		log.info('adding poseidon\'s key');

		assert.object(POSEIDON, 'POSEIDON');
		assert.string(POSEIDON_LOGIN, 'POSEIDON.LOGIN');

		if (app.metadata['ADMIN_PRIVATE_KEY']) {
			log.info('SSH key already present, not generating');
			return (cb(null));
		}

		var keyfile = sprintf('/tmp/key.%d.rsa', process.pid);
		var pubfile = keyfile + '.pub';

		async.waterfall([
			function (subcb) {
				ssh.generateKey.call(self, keyfile, subcb);
			},
			function (key, subcb) {
				ssh.addPublicKey.call(self,
				    POSEIDON, pubfile, function (err) {
					subcb(err, key);
				});
			},
			function (key, subcb) {
				var metadata = {};
				metadata['ADMIN_USERNAME'] = POSEIDON_LOGIN;
				metadata['ADMIN_PRIVATE_KEY'] = key.priv;
				metadata['ADMIN_PUBLIC_KEY'] = key.pub;
				metadata['ADMIN_KEY_ID'] = key.id;

				sapi.updateApplication(app.uuid,
				    { metadata: metadata }, function (err) {
					if (err) {
						log.error(err,
						    'failed to push SSH keys');
						return (subcb(err));
					}

					subcb();
				});
			}
		], function (err) {
			/*
			 * Even if there's an error, remove the SSH public key
			 * file.
			 */
			fs.unlink(pubfile, function () {
				cb(err);
			});
		});
	},

	function addApplicationConfigs(_, cb) {
		var log = self.log;
		var sapi = self.SAPI;

		var uuid = self.sapi_application.uuid;
		var dirname = path.join(path.dirname(__filename),
		    '../manifests/applications/manta');
		var updatefunc = sapi.updateApplication.bind(sapi, uuid);

		addConfig(dirname, updatefunc, function (err) {
			if (err) {
				log.error(err, 'failed to load manifests ' +
				    'for %s', uuid);
				return (cb(err));
			}

			log.info('loaded manifests from %s', dirname);
			return (cb(null));
		});
	},

	function determineDefaultChannel(_, cb) {
		var log = self.log;
		// If the user passed a -C argument, then we're done
		if (ARGV.channel !== undefined) {
			return (cb(null));
		}

		log.info('determining update_channel from sapi');
		var opts = {
			sapi: self.SAPI,
			log: self.log
		};
		common.getSdcChannel(opts,
			function (err, channel) {
			if (!err) {
				ARGV.channel = channel;
				return (cb(null));
			}
			log.error(
			    err, 'failed to determine sdc update_channel');
			return (cb(err));
		});
	},

	function findLatestImages(ctx, cb) {
		var log = self.log;

		log.info({ services: services.mSvcNames },
		    'finding images for services');

		/*
		 * If the marlin image was given, we filter it out and add it
		 * manually later.
		 */
		var filtered = services.mSvcNames.filter(function f(svcname) {
			if (ARGV.m && svcname === 'marlin') {
				return (false);
			}
			return (true);
		});

		vasync.forEachParallel({
			func: findLatestImage,
			inputs: filtered
		}, function (suberr, results) {
			var rs = results.successes;
			var images = rs.map(function (im) {
				return ({
					'uuid': im.uuid,
					'name': im.name,
					'origin': im.origin
				});
			});

			// Adding the marlin image manually.
			if (ARGV.m) {
				images.push({
					'uuid': ARGV.m,
					'name': services.serviceNameToImageName(
					    'marlin')
				});
			}

			ctx.images = images;
			return (cb(suberr));
		});
	},

	/*
	 * Importing all the Manta images into this DC's IMGAPI hits a common
	 * issue. We do concurrent image imports to speed up the process.
	 * However, if two concurrent image imports have the same *origin*
	 * image, IMGAPI can hit a limitation: IMGAPI will be importing the
	 * origin for the first image. Then, when the second image begins
	 * importing, IMGAPI can, with unlucky timing, notice that the origin
	 * image *exists* but is incomplete (state=unactivated), and it will
	 * error out:
	 *
	 * 	OriginIsNotActiveError: origin image "..." is not active
	 *
	 * or hit this similar unlucky timing error:
	 *
	 * 	Error: image uuid "<origin image uuid>" already exists
	 *
	 * Until IMGAPI supports this (TRITON-1766), some work around options
	 * are:
	 *
	 * 1. Retry the failed image imports.
	 * 2. Have a leading stage that determines all the shared origin
	 *    images and imports those serially first.
	 *
	 * This step does #2.
	 */
	function importImageOrigins(ctx, cb) {
		var imgapi = self.IMGAPI;
		var log = self.log;
		var origin_images;
		var remote_url = self.config.remote_imgapi.url;
		var images = ctx.images;

		assert.arrayOfObject(images, 'images');
		assert.func(cb, 'cb');

		origin_images = {};
		images.forEach(function (im) {
			if (im.origin) {
				origin_images[im.origin] = true;
			}
		});
		origin_images = Object.keys(origin_images);

		log.info({origin_images: origin_images, remote_url: remote_url},
			'downloading origin images');

		vasync.forEachPipeline({
			inputs: origin_images,
			func: function importOneOriginImage(uuid, subcb) {
				log.info('downloading origin image %s', uuid);
				function onDoneOneImage(err) {
					if (err && err.name !==
					    'ImageUuidAlreadyExistsError') {
						log.error({err: err,
						    image_uuid: uuid},
						    'failed to download image');
						subcb(err);
						return;
					} else if (err) {
						log.info('origin image %s ' +
							'already downloaded',
							uuid);
					} else {
						log.info('downloaded origin ' +
							'image %s', uuid);
					}
					subcb();
				}
				imgapi.adminImportRemoteImageAndWait(
					uuid, remote_url, {}, onDoneOneImage);
			}
		}, function (err) {
			cb(err);
		});
	},

	function importImages(ctx, cb) {
		var imgapi = self.IMGAPI;
		var log = self.log;
		var remote_url = self.config.remote_imgapi.url;
		var images = ctx.images;

		assert.arrayOfObject(images, 'images');
		assert.func(cb, 'cb');

		log.info({ images: images, remote_url: remote_url },
			'downloading images');

		// De-dupe images so we don't ask IMGAPI to concurrently
		// download the same image twice.
		var image_from_uuid = {};
		images.forEach(
			function (img) { image_from_uuid[img.uuid] = img; });

		async.forEachLimit(Object.keys(image_from_uuid), CONCURRENCY,
		    function (image_uuid, subcb) {
			var import_opts = {};
			import_opts.skipOwnerCheck = true;

			log.info('downloading image %s', image_uuid);

			function onDone(err, img, res) {
				if (err && err.name !==
				    'ImageUuidAlreadyExistsError') {
					log.error({err: err,
						image_uuid: image_uuid},
						'failed to download image');
					return (subcb(err));
				} else if (err) {
					log.info('image %s already downloaded',
						image_uuid);
				} else {
					log.info('downloaded image %s',
						image_uuid);
				}
				return (subcb());
			}

			if (ARGV.m && image_from_uuid[image_uuid].name
			    === 'manta-marlin') {
				image_uuid = ARGV.m;
			}

			imgapi.adminImportRemoteImageAndWait(
				image_uuid, remote_url, import_opts, onDone);

		}, function (err) {
			return (cb(err));
		});
	},

	function createMantaServices(ctx, cb) {
		var sapi = self.SAPI;
		var log = self.log;
		var app_uuid = self.sapi_application.uuid;
		var images = ctx.images;

		assert.arrayOfObject(images, 'images');
		assert.func(cb, 'cb');

		log.debug({ images: images }, 'creating services');

		/*
		 * We're going to look up images by name below, so we put them
		 * in an object now to avoid doing repeated linear lookups.
		 */
		var imgMap = {};
		images.forEach(function addToMap(img) {
			imgMap[img.name] = img;
		});

		vasync.forEachParallel({
			func: function (svcname, subcb) {
				var file = sprintf(
				    '%s/../config/services/%s/service.json',
				    path.dirname(__filename), svcname);
				file = path.resolve(file);
				var override = file + '.' + ARGV.s;

				var files = [ file, override ];

				var extra = {};
				extra.params = {};

				var imgName =
				    services.serviceNameToImageName(svcname);
				assert.object(imgMap[imgName],
				    'imgMap[imgName]');
				extra.params.image_uuid = imgMap[imgName].uuid;

				extra.master = true;
				extra.include_master = true;

				log.info('getting or creating service %s ' +
					'with uuid %s', svcname, app_uuid);

				sapi.getOrCreateService(svcname, app_uuid,
				    files, extra, function (err, svc) {
					/*
					 * We hang the image off of the service
					 * object so we can get a service's
					 * image in the next function.
					 */
					if (!err)
						svc.image = imgMap[imgName];
					return (subcb(err, svc));
				});
			},
			inputs: services.mSvcNames
		}, function (err, results) {
			if (err)
				return (cb(err));

			ctx.sapi_services = results.successes;

			log.debug('created %d SAPI services',
			    ctx.sapi_services.length);

			return (cb(null));
		});
	},

	function updateServiceImages(ctx, cb) {
		var log = self.log;
		var sapi_services = ctx.sapi_services;

		assert.arrayOfObject(sapi_services);
		assert.func(cb, 'cb');

		vasync.forEachParallel({
			func: function (svc, subcb) {
				updateServiceImage.call(self,
				    svc, svc.image.uuid, subcb);
			},
			inputs: sapi_services
		}, function (err) {
			if (err)
				return (cb(err));
			log.info('updated all images on services');
			return (cb(null));
		});
	},

	function addMuskieAes(ctx, cb) {
		var log = self.log;
		var sapi = self.SAPI;
		var cmd = 'openssl enc -aes-128-cbc -k ' + node_uuid.v4() +
			' -P';
		var pfx = 'MUSKIE_JOB_TOKEN_AES_';
		var svc, i, m;
		var sapi_services = ctx.sapi_services;

		assert.arrayOfObject(sapi_services);
		assert.func(cb, 'cb');

		for (i = 0; i < sapi_services.length; i++) {
			if (sapi_services[i].name == 'webapi') {
				svc = sapi_services[i];
				break;
			}
		}

		if (svc === undefined) {
			m = 'did not find expected "webapi" service!';
			log.error(m);
			cb(new Error(m));
			return;
		}

		if (svc.metadata.hasOwnProperty('MUSKIE_JOB_TOKEN_AES_KEY')) {
			log.info('skipping muskie AES key (already present)');
			cb(null);
			return;
		}

		log.info('generating muskie AES key');
		async.waterfall([
			/*
			 * Turns output of OpenSSL key gen into
			 * an object:
			 *
			 * salt=ABDABC20E045270D
			 * key=10E0E4E7F8AF968E22819E91AA7D45E9
			 * iv =4A603A273291A459A60C5AB240E9CEC2
			 *
			 * {
			 *   salt: 'ABDABC20E045270D',
			 *   key: '10E0E4E7F8AF968E22819E91AA7D45E9',
			 *   iv: '4A603A273291A459A60C5AB240E9CEC2'
			 * }
			 */
			function (subcb) {
				child_process.exec(cmd, function (err, stdout) {
					if (err) {
						subcb(err);
						return;
					}

					var aes = {};
					var lines = stdout.split('\n');
					lines.forEach(function (l) {
						var tmp = l.split('=');
						aes[tmp[0].trim()] = tmp[1];
					});

					subcb(null, aes);
				});
			},
			function (aes, subcb) {
				var md = {};
				Object.keys(aes).forEach(function (k) {
					md[pfx + k.toUpperCase()] = aes[k];
				});

				sapi.updateService(svc.uuid,
				    { metadata: md }, function (err) {
					if (err) {
						log.error(err,
						    'failed to push ' +
						    'job token keys ' +
						    '(muskie)');
						subcb(err);
						return;
					}

					subcb();
				});
			}
		], cb);
	},

	function finiClients(_, cb) {
		common.finiSdcClients.call(self, cb);
	}
];

vasync.pipeline({
	arg: {}, // ctx
	funcs: pipelineFuncs
}, function (err) {
	if (err) {
		console.error('Error: ' + err.message);
		process.exit(1);
	}
});
