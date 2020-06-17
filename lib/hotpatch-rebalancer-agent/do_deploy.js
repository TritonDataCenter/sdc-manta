/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * `manta-hotpatch-rebalancer-agent deploy ...`
 *
 * Command to deploy a given rebalancer-agent image as a hotpatch to storage
 * instances.
 */

var assert = require('assert-plus');
var fs = require('fs');
var util = require('util');

var cmdln = require('cmdln');
var vasync = require('vasync');
var VError = require('verror');

var common = require('../common');
var clicommon = require('../clicommon');

function do_deploy(subcmd, opts, args, cb) {
    let hotpatcher = this.hotpatcher;
    let imageUuid;
    let log = this.log;
    let ui = this.ui;

    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    } else if (args.length < 1) {
        cb(new cmdln.UsageError('missing IMAGE-UUID argument'));
        return;
    } else if (args.length > 1) {
        cb(new cmdln.UsageError('too many arguments'));
        return;
    }

    imageUuid = args[0];
    if (!common.isUuid(imageUuid)) {
        cb(new cmdln.UsageError('"' + imageUuid + '" is not a UUID'));
        return;
    }
    if (opts.all && opts.storage_instances) {
        cb(new cmdln.UsageError('cannot specify both -a and -s'));
        return;
    } else if (opts.all) {
        log.debug('deploy targets: "all" storage instances');
    } else if (opts.storage_instances) {
        log.debug('deploy targets: %j', opts.storage_instances);
    } else {
        cb(new cmdln.UsageError('must specify -a or "-s ..."'));
        return;
    }

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function validateStorInsts(ctx, next) {
                    hotpatcher.listStorInsts(function onInst(
                        err,
                        allStorInsts
                    ) {
                        if (err) {
                            next(err);
                            return;
                        }

                        if (allStorInsts.length === 0) {
                            next(
                                new VError(
                                    'there are no storage instances in this DC to which to deploy rebalancer-agent'
                                )
                            );
                            return;
                        } else if (opts.all) {
                            ctx.storInsts = allStorInsts;
                        } else if (opts.storage_instances.length === 0) {
                            next(
                                new VError(
                                    '"-s, --storage-instances" argument cannot be an empty list'
                                )
                            );
                            return;
                        } else {
                            let storInstFromUuid = {};
                            for (let inst of allStorInsts) {
                                storInstFromUuid[inst.uuid] = inst;
                            }

                            ctx.storInsts = [];
                            for (let uuid of opts.storage_instances) {
                                let inst = storInstFromUuid[uuid];
                                if (inst) {
                                    ctx.storInsts.push(inst);
                                } else {
                                    next(
                                        new VError(
                                            'there is no storage instance with UUID "%s" in this DC',
                                            uuid
                                        )
                                    );
                                    return;
                                }
                            }
                        }

                        next();
                    });
                },

                function checkForLocalImage(ctx, next) {
                    hotpatcher.getLocalImage(imageUuid, function onImg(
                        err,
                        img
                    ) {
                        if (err) {
                            if (err.name === 'ResourceNotFoundError') {
                                next();
                            } else {
                                next(
                                    new VError(
                                        err,
                                        'error getting image %s from the DC IMGAPI',
                                        imageUuid
                                    )
                                );
                            }
                        } else if (img.name !== 'mantav2-rebalancer-agent') {
                            next(
                                new VError(
                                    'image %s is not name=mantav2-rebalancer-agent: name=%s',
                                    imageUuid,
                                    img.name
                                )
                            );
                        } else {
                            ctx.localImg = img;
                            assert.equal(
                                img.state,
                                'active',
                                'local image ' +
                                    imageUuid +
                                    'is active: state=' +
                                    img.state
                            );
                            next();
                        }
                    });
                },

                function checkForRemoteImageIfNecessary(ctx, next) {
                    if (ctx.localImg) {
                        next();
                        return;
                    }

                    hotpatcher.getRemoteImage(imageUuid, function onImg(
                        err,
                        img
                    ) {
                        if (err) {
                            next(
                                new VError(
                                    err,
                                    'error getting image %s from updates.joyent.com (dev channel)',
                                    imageUuid
                                )
                            );
                        } else if (img.name !== 'mantav2-rebalancer-agent') {
                            next(
                                new VError(
                                    'image %s is not name=mantav2-rebalancer-agent: name=%s',
                                    imageUuid,
                                    img.name
                                )
                            );
                        } else {
                            ctx.remoteImg = img;
                            next();
                        }
                    });
                },

                function confirm(ctx, next) {
                    ui.info('This will do the following:');
                    if (!ctx.localImg) {
                        ui.infoBullet(
                            'Import rebalancer-agent image %s (%s) from updates.joyent.com.',
                            ctx.remoteImg.uuid,
                            ctx.remoteImg.version
                        );
                    }

                    var img = ctx.localImg || ctx.remoteImg;
                    var extra = '';
                    if (!opts.all) {
                        // Print some details about the selected storage nodes
                        // to be hotpatched (to help with confirmation).
                        extra = ':';
                        for (let inst of ctx.storInsts.slice(0, 10)) {
                            extra += util.format(
                                '\n    - %s (%s)',
                                inst.uuid,
                                inst.metadata.MANTA_STORAGE_ID
                            );
                        }
                        if (ctx.storInsts.length > 10) {
                            extra += util.format(
                                '\n    ... %d more instances',
                                ctx.storInsts.length - 10
                            );
                        }
                    }
                    ui.infoBullet(
                        'Hotpatch rebalancer-agent image %s (%s) on %s%d storage instance%s in this DC%s',
                        img.uuid,
                        img.version,
                        opts.all ? 'all ' : '',
                        ctx.storInsts.length,
                        ctx.storInsts.length !== 1 ? 's' : '',
                        extra
                    );
                    ui.info('');

                    if (opts.y) {
                        next();
                        return;
                    }

                    clicommon.promptYesNo(
                        {
                            msg: 'Would you like to hotpatch? [y/N] ',
                            default: 'n'
                        },
                        function onPrompted(answer) {
                            if (answer !== 'y') {
                                ui.info('Aborting.');
                                next(new VError('user abort'));
                            } else {
                                next();
                            }
                        }
                    );
                },

                function addTraceLogToFile(_, next) {
                    var timestamp =
                        new Date()
                            .toISOString()
                            .replace(/-/g, '')
                            .replace(/:/g, '')
                            .split('.')[0] + 'Z';
                    var logFile = `/var/tmp/manta-hotpatch-rebalancer-agent.${timestamp}.deploy.log`;
                    ui.info('Trace logging to "%s"', logFile);
                    log.addStream({level: 'trace', path: logFile});
                    next();
                },

                function importImageIfNecessary(ctx, next) {
                    if (ctx.localImg) {
                        next();
                        return;
                    }

                    ui.info(
                        'Importing image %s from updates.joyent.com',
                        ctx.remoteImg.uuid
                    );
                    hotpatcher.importImage(
                        ctx.remoteImg.uuid,
                        function onImported(err, img) {
                            if (err) {
                                next(
                                    new VError(
                                        err,
                                        'could not import image %s from updates.joyent.com',
                                        ctx.remoteImg.uuid
                                    )
                                );
                            } else {
                                ui.info('Imported image');
                                ctx.localImg = img;
                                next();
                            }
                        }
                    );
                },

                function deployAway(ctx, next) {
                    hotpatcher.hotpatchRebalAgents(
                        {
                            storInsts: ctx.storInsts,
                            imageUuid: ctx.localImg.uuid,
                            ui: ui,
                            concurrency: opts.concurrency
                        },
                        next
                    );
                },

                function noteSuccess(_, next) {
                    ui.info('Successfully hotpatched.');

                    if (subcmd === 'hansel') {
                        process.stdout.write(
                            fs.readFileSync(
                                __dirname + '/so-hot-right-now.ansi'
                            )
                        );
                    }

                    next();
                }
            ]
        },
        function finish(err) {
            if (err === true) {
                // Early abort signal.
                err = null;
            }
            cb(err);
        }
    );
}

do_deploy.options = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Show this help.'
    },
    {
        names: ['y'],
        type: 'bool',
        help: 'Answer yes to confirmation.'
    },
    {
        names: ['concurrency', 'c'],
        type: 'positiveInteger',
        help:
            'The number of storage instance on which to run ' +
            'concurrently. By default this is 100.',
        default: 100
    },
    {
        group: 'Select storage instances'
    },
    {
        names: ['all', 'a'],
        type: 'bool',
        help:
            'Deploy to all storage instances. One of "-a" or "-s ..." must ' +
            'specified.'
    },
    {
        names: ['storage-instances', 's'],
        type: 'arrayOfCommaSepString',
        help:
            'Deploy to the given storage instances (a comma-separated list ' +
            'of storage instance UUIDs). This option can be given multiple ' +
            'times. One of "-a" or "-s ..." must specified.'
    }
];

do_deploy.help = [
    'Deploy the given rebalancer-agent image hotpatch to storage instances.',
    '',
    '{{usage}}',
    '',
    '{{options}}'
].join('\n');

do_deploy.helpOpts = {
    helpCol: 20
};

do_deploy.hiddenAliases = ['hansel'];
do_deploy.synopses = ['{{name}} {{cmd}} [OPTIONS] IMAGE-UUID'];
do_deploy.completionArgtypes = ['default', 'none'];

module.exports = do_deploy;
