/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * ssh.js: create and manage SSH keys
 */

var assert = require('assert-plus');
var async = require('async');
var fs = require('fs');

var exec = require('child_process').exec;
var sprintf = require('util').format;

// -- Exported interface

exports.generateKey = function generateKey(keyfile, cb) {
    assert.string(keyfile, 'keyfile');
    assert.func(cb, 'cb');

    var log = this.log;

    var key = {};

    async.waterfall(
        [
            function(subcb) {
                var cmd = sprintf(
                    '/usr/bin/ssh-keygen -t rsa -m PEM -f %s -N ""',
                    keyfile
                );

                log.info({cmd: cmd}, 'generating SSH key');

                exec(cmd, function(err, _stdout, _stderr) {
                    if (err) {
                        log.error(err, 'failed to generate SSH key');
                        subcb(err);
                        return;
                    }

                    subcb(null);
                });
            },
            function(subcb) {
                log.info('reading private key from %s', keyfile);

                fs.readFile(keyfile, 'ascii', function(err, contents) {
                    if (err) {
                        log.error(
                            err,
                            'failed to read private key %s',
                            keyfile
                        );
                        subcb(err);
                        return;
                    }

                    key.priv = contents.trim();
                    subcb(null);
                });
            },
            function(subcb) {
                var pubfile = keyfile + '.pub';

                log.info('reading public key from %s', pubfile);

                fs.readFile(pubfile, 'ascii', function(err, contents) {
                    if (err) {
                        log.error(err, 'failed to read public key %s', pubfile);
                        subcb(err);
                        return;
                    }

                    key.pub = contents.trim();
                    subcb(null);
                });
            },
            function(subcb) {
                var cmd = sprintf(
                    '/usr/bin/ssh-keygen -l -f %s | ' + "awk '{print $2}'",
                    keyfile
                );

                log.info({cmd: cmd}, 'reading key signature');

                exec(cmd, function(err, stdout, _stderr) {
                    if (err) {
                        log.error(err, 'failed to read key signature');
                        subcb(err);
                        return;
                    }

                    key.id = stdout.trim();
                    subcb(null);
                });
            }
        ],
        function(err) {
            /*
             * Remove the private key file, but leave the public key for the
             * caller to read.
             */
            fs.unlink(keyfile, function(_unlinkErr) {
                cb(err, key);
            });
        }
    );
};

exports.addPublicKey = function addPublicKey(user, keyfile, cb) {
    var ufds = this.UFDS;
    var log = this.log;

    assert.object(user, 'user');
    assert.string(user.login, 'user.login');
    assert.string(keyfile, 'keyfile');
    assert.func(cb, 'cb');

    fs.readFile(keyfile, 'ascii', function(err, key) {
        if (err) {
            log.error(err, 'failed to read SSH public key %s', keyfile);
            return cb(err);
        }

        ufds.addKey(user, key, function(suberr) {
            if (suberr && suberr.restCode !== 'InvalidArgument') {
                log.error(
                    suberr,
                    'failed to add SSH public key for %s',
                    user.login
                );
                return cb(suberr);
            } else if (suberr) {
                /*
                 * An InvalidArgumentError indicates (among
                 * other things) that this SSH key is has
                 * already been added for the user.
                 */
                assert.ok(suberr.restCode === 'InvalidArgument');
                log.warn(
                    suberr,
                    'failed to add SSH public key for %s',
                    user.login
                );
            }

            return cb(null);
        });

        return null;
    });
};
