/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 * Copyright 2022 MNX Cloud, Inc.
 */

/*
 * `manta-hotpatch-rebalancer-agent avail ...`
 *
 * Command to list newer available "rebalancer-agent" images to use for
 * hotpatching.
 */

var cmdln = require('cmdln');
var tabula = require('tabula');
var vasync = require('vasync');

// Globals
let columnsDefault = ['uuid', 'name', 'version', 'published_at'];

//
// Given an array return a string with each element JSON-stringifed separated by
// newlines.
//
function jsonStream(arr, stream) {
    stream = stream || process.stdout;

    arr.forEach(function(elem) {
        stream.write(JSON.stringify(elem) + '\n');
    });
}

function do_avail(subcmd, opts, args, cb) {
    let columns;
    let hotpatcher = this.hotpatcher;
    let log = this.log;
    let sort;
    let ui = this.ui;

    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    } else if (args.length !== 0) {
        cb(new cmdln.UsageError('unexpected arguments'));
        return;
    }

    if (opts.o) {
        columns = opts.o.split(',');
    } else {
        columns = columnsDefault;
    }
    sort = opts.s.split(',');

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function getAllStorInsts(ctx, next) {
                    hotpatcher.listStorInsts(function onInst(err, insts) {
                        if (err) {
                            next(err);
                        } else if (insts.length === 0) {
                            ui.error('No rebalancer-agents are deployed.');
                            next(true);
                        } else {
                            ctx.storInsts = insts;
                            next();
                        }
                    });
                },

                function getRebalAgentInfo(ctx, next) {
                    hotpatcher.listRebalAgents(ctx.storInsts, function onInfo(
                        err,
                        rebalAgents
                    ) {
                        if (err) {
                            next(err);
                        } else {
                            ctx.rebalAgents = rebalAgents;
                            next();
                        }
                    });
                },

                function getOldestRebalAgentBuildDate(ctx, next) {
                    // The rebalAgent.version looks like this:
                    //      0.1.0 (master-20200602T194719Z-g0c96e91)
                    //                    ^^^^^^^^^^^^^^^^
                    //                          '-- (the build date)
                    // or like this for rebalancer-agents before MANTA-5292
                    // (2020-06-02):
                    //      0.1.0
                    //
                    // If the former, then we have a build date to work with.
                    let verRe = /^\d+\.\d+\.\d+( \([\w-]+-(\d{8}T\d{6}Z)-g[0-9a-f]{7}\))?$/;
                    let oldestTime = null;
                    for (let rebalAgent of ctx.rebalAgents) {
                        let parsed = verRe.exec(rebalAgent.version);
                        if (!parsed) {
                            log.debug(
                                {rebalAgent: rebalAgent, verRe: verRe},
                                'could not parse rebalAgent.version'
                            );
                            ui.error(
                                'Could not parse rebalAgent version "%s"',
                                rebalAgent.version
                            );
                        } else if (
                            parsed[1] &&
                            (!oldestTime || parsed[2] < oldestTime)
                        ) {
                            oldestTime = parsed[2];
                        }
                    }

                    ctx.oldestTime = oldestTime;
                    next();
                },

                function listAvailImages(ctx, next) {
                    let marker = null;

                    if (ctx.oldestTime) {
                        // oldestTime looks like '20200602T194719Z', but IMGAPI
                        // wants a form that `new Date()` can parse, e.g.:
                        // `2020-06-02T19:47:19Z`.
                        let t = ctx.oldestTime;
                        marker =
                            t.slice(0, 4) +
                            '-' +
                            t.slice(4, 6) +
                            '-' +
                            t.slice(6, 8) +
                            'T' +
                            t.slice(9, 11) +
                            ':' +
                            t.slice(11, 13) +
                            ':' +
                            t.slice(13, 15) +
                            'Z';
                    }

                    hotpatcher.listAvailImages(marker, function(
                        err,
                        availImages
                    ) {
                        ctx.availImages = availImages;
                        next(err);
                    });
                },

                function printTable(ctx, next) {
                    if (opts.json) {
                        jsonStream(ctx.availImages);
                    } else {
                        tabula(ctx.availImages, {
                            dottedLookup: true,
                            skipHeader: opts.H,
                            columns: columns,
                            sort: sort
                        });
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

do_avail.options = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Show this help.'
    },
    {
        group: 'Output options'
    },
    {
        names: ['H'],
        type: 'bool',
        help: 'Omit table header row.'
    },
    {
        names: ['o'],
        type: 'string',
        help:
            'Specify fields (columns) to output. See "-j" output for field ' +
            'names.',
        helpArg: 'field1,...'
    },
    {
        names: ['s'],
        type: 'string',
        help: 'Sort on the given fields. Prefix a field with "-" to reverse.',
        helpArg: 'field1,...',
        default: 'published_at'
    },
    {
        names: ['json', 'j'],
        type: 'bool',
        help: 'JSON output.'
    }
];

do_avail.help = [
    'List newer available images for hotpatching rebalancer-agent.',
    '',
    '{{usage}}',
    '',
    '{{options}}',
    'This lists rebalancer-agent images available in',
    'updates.tritondatacenter.com that are newer than the oldest',
    'rebalancer-agent build currently deployed in this DC. This is',
    '*hotpatching*, so the only updates.tritondatacenter.com channel we are',
    'considering is the default "dev" channel.'
].join('\n');

do_avail.synopses = ['{{name}} {{cmd}} [OPTIONS]'];
do_avail.completionArgtypes = ['none'];

module.exports = do_avail;
