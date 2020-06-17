/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * `manta-hotpatch-rebalancer-agent undeploy ...`
 *
 * Command to restore the original rebalancer-agent in storage instances.
 * I.e., this undoes the hotpatching of rebalancer-agent.
 */

var assert = require('assert-plus');
var fs = require('fs');
var util = require('util');

var cmdln = require('cmdln');
var vasync = require('vasync');
var VError = require('verror');

var common = require('../common');
var clicommon = require('../clicommon');

function do_undeploy(subcmd, opts, args, cb) {
    let hotpatcher = this.hotpatcher;
    let imageUuid;
    let log = this.log;
    let ui = this.ui;

    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    } else if (args.length > 0) {
        cb(new cmdln.UsageError('too many arguments'));
        return;
    }

    if (opts.all && opts.storage_instances) {
        cb(new cmdln.UsageError('cannot specify both -a and -s'));
        return;
    } else if (opts.all) {
        log.debug('undeploy targets: "all" storage instances');
    } else if (opts.storage_instances) {
        log.debug('undeploy targets: %j', opts.storage_instances);
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

                function confirm(ctx, next) {
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
                    ui.info(
                        'This will revert any rebalancer-agent hotpatches on %s%d storage instance%s in this DC%s',
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
                            msg: 'Would you like to continue? [y/N] ',
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
                    var logFile = `/var/tmp/manta-hotpatch-rebalancer-agent.${timestamp}.undeploy.log`;
                    ui.info('Trace logging to "%s"', logFile);
                    log.addStream({level: 'trace', path: logFile});
                    next();
                },

                function undeployAway(ctx, next) {
                    hotpatcher.unhotpatchRebalAgents(
                        {
                            storInsts: ctx.storInsts,
                            ui: ui,
                            concurrency: opts.concurrency
                        },
                        next
                    );
                },

                function noteSuccess(_, next) {
                    ui.info('Successfully reverted hotpatches.');
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

do_undeploy.options = [
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
        help: 'The number of storage instance on which to run ' +
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
            'Undeploy on all storage instances. One of "-a" or "-s ..." must ' +
            'specified.'
    },
    {
        names: ['storage-instances', 's'],
        type: 'arrayOfCommaSepString',
        help:
            'Undeploy on the given storage instances (a comma-separated list ' +
            'of storage instance UUIDs). This option can be given multiple ' +
            'times. One of "-a" or "-s ..." must specified.'
    }
];

do_undeploy.help = [
    'Undo the rebalancer-agent hotpatch on storage instances.',
    '',
    '{{usage}}',
    '',
    '{{options}}',
    'This command will revert the rebalancer-agent back to the original',
    'version that is part of the storage image.'
].join('\n');

do_undeploy.helpOpts = {
    helpCol: 20
};

do_undeploy.synopses = ['{{name}} {{cmd}} [OPTIONS]'];
do_undeploy.completionArgtypes = ['default', 'none'];

module.exports = do_undeploy;
