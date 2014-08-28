/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * lib/ssl.js: create an SSL certificate
 */

var assert = require('assert-plus');
var fs = require('fs');

var exec = require('child_process').exec;
var sprintf = require('util').format;


// -- Exported interface

exports.generateCertificate = generateCertificate;

function generateCertificate(outfile, service_name, cb) {
	var log = this.log;

	assert.string(outfile, 'outfile');
	assert.string(service_name, 'service_name');
	assert.func(cb, 'cb');

	var cmd = sprintf('/usr/bin/openssl req -x509 -nodes -days 365 ' +
	    '-newkey rsa:2048 -keyout %s -out %s ' +
	    '-config /opt/local/etc/openssl/openssl.cnf ' +
	    '-subj "/C=US/ST=CA/O=Joyent/OU=manta/CN=%s"',
	    outfile, outfile, service_name);

	log.info({ cmd: cmd }, 'generating SSL certificate');

	exec(cmd, function (err, stdout, stderr) {
		if (err) {
			log.error(err, 'failed to generate SSL certificate');
			return (cb(err));
		}

		return (cb(null));
	});

	return (null);
}
