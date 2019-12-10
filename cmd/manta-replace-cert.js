#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * manta-replace-cert.js: replace the manta SSL certificate.
 */

var common = require('../lib/common');
var fs = require('fs');
var Logger = require('bunyan');
var optimist = require('optimist');
var vasync = require('vasync');

function usage() {
    optimist.showHelp();
}

function fatal(err) {
    console.error('Error: ' + err.message);
    process.exit(1);
}

var ARGV;
var bstreams;
var options;
var log;

var use = [
    'Usage:\tmanta-replace-cert <certificate.pem>',
    '',
    'Certificates should be PEM encoded, and contain the public and ',
    'private keys as well as the certificate chain.  When viewing your',
    'file, it should have the following format:',
    '',
    '-----BEGIN RSA PRIVATE KEY-----',
    '[Base64 Encoded Private Key]',
    '-----END RSA PRIVATE KEY-----',
    '-----BEGIN CERTIFICATE-----',
    '[Base64 Encoded Certificate]',
    '-----END CERTIFICATE-----',
    '',
    'Note that there may be multiple CERTIFICATE sections.  Each of these',
    'will be a certificate in your trust chain and should be organized',
    'so that your leaf-certificate is first, and your root certificate',
    'is last.',
    '',
    'Warning: This does *no* certificate validation.  Before you start,',
    '	 you can backup your current cert with the following',
    '	 command:',
    'headnode$ sdc-sapi /services?name=loadbalancer | \\',
    '    json -Ha metadata.SSL_CERTIFICATE >/var/tmp/ssl_cert.pem'
].join('\n');
optimist.usage(use);
ARGV = optimist.options({
    l: {
        alias: 'log_file',
        describe: 'dump logs to this file (or "stdout")',
        default: '/var/log/manta-replace-cert.log'
    }
}).argv;

if (ARGV._.length !== 1) {
    usage();
    process.exit(2);
}

var certFile = ARGV._[0];

if (ARGV.l === 'stdout') {
    bstreams = [
        {
            level: 'debug',
            stream: process.stdout
        }
    ];
} else {
    bstreams = [
        {
            level: 'debug',
            path: ARGV.l
        }
    ];
    console.error('logs at ' + ARGV.l);
}

log = new Logger({
    name: 'manta-replace-cert',
    serializers: Logger.stdSerializers,
    streams: bstreams
});

var funcs = [
    function readPem(_, cb) {
        fs.readFile(certFile, 'utf8', function(err, certString) {
            _.certString = certString;
            return cb(err);
        });
    },
    function initSdcClients(_, cb) {
        _.log = log;
        common.initSdcClients.call(_, cb);
    },
    function getPoseidon(_, cb) {
        _.UFDS.getUser('poseidon', function(err, user) {
            if (err && err.name === 'ResourceNotFoundError') {
                console.log('No manta installation found');
                return cb(err);
            } else if (err) {
                log.error(err, 'failed to get poseidon user');
                return cb(err);
            }

            _.poseidon = user;
            return cb(null);
        });
    },
    function getMantaApplication(_, cb) {
        var owner = _.poseidon.uuid;
        common.getMantaApplication.call(_, owner, function(err, app) {
            if (app) {
                _.application = app;
            }
            cb(err);
        });
    },
    function getLoadBalancerService(_, cb) {
        var query = {
            owner_uuid: _.poseidon.uuid,
            application_uuid: _.application.uuid,
            name: 'loadbalancer',
            include_master: true
        };
        _.SAPI.listServices(query, function(err, services) {
            if (err) {
                return cb(err);
            }
            if (!services || services.length !== 1) {
                var m = 'failed to find loadbalancer service';
                return cb(new Error(m));
            }
            _.service = services[0];
            return cb();
        });
    },
    function replaceCertificate(_, cb) {
        var service = _.service.uuid;
        var changes = {
            metadata: {
                SSL_CERTIFICATE: _.certString
            }
        };
        _.SAPI.updateService(service, changes, function(err) {
            if (err) {
                log.error(err, 'failed to update certificate');
            }
            return cb(err);
        });
    }
];

vasync.pipeline(
    {
        arg: {},
        funcs: funcs
    },
    function(err) {
        if (err) {
            console.error(err);
            process.exit(1);
        }
        process.exit(0);
    }
);
