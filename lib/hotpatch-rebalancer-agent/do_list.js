/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * `manta-hotpatch-rebalancer-agent list ...`
 *
 * Command to list the current version of rebalancer-agent on storage instances.
 */

var cmdln = require('cmdln');
var tabula = require('tabula');
var vasync = require('vasync');

// Globals
let columnsDefault = [
    {
        lookup: 'storInst.uuid',
        name: 'STORAGE NODE'
    },
    {
        lookup: 'version'
    },
    {
        lookup: 'hotpatched'
    }
];
let columnsDefaultLong = [
    {
        lookup: 'storInst.uuid',
        name: 'STORAGE NODE'
    },
    {
        lookup: 'storInst.metadata.MANTA_STORAGE_ID',
        name: 'MANTA_STORAGE_ID'
    },
    {
        lookup: 'storImageUuid',
        name: 'STORAGE IMAGE'
    },
    {
        lookup: 'version'
    },
    {
        lookup: 'hotpatched'
    }
];

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

function do_list(subcmd, opts, args, cb) {
    let columns;
    let hotpatcher = this.hotpatcher;
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
    } else if (opts.long) {
        columns = columnsDefaultLong;
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

                function printTable(ctx, next) {
                    if (opts.json) {
                        jsonStream(ctx.rebalAgents);
                    } else {
                        tabula(ctx.rebalAgents, {
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

do_list.options = [
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
            'names. Dotted-lookup is supported, e.g. "storInst.params.alias".',
        helpArg: 'field1,...'
    },
    {
        names: ['long', 'l'],
        type: 'bool',
        help:
            'Long output (showing more common fields). ' +
            'Ignored if "-o ..." is used.'
    },
    {
        names: ['s'],
        type: 'string',
        help: 'Sort on the given fields. Prefix a field with "-" to reverse.',
        helpArg: 'field1,...',
        default: 'hotpatched,version,storInst.uuid'
    },
    {
        names: ['json', 'j'],
        type: 'bool',
        help: 'JSON output.'
    }
];

do_list.help = [
    'List running rebalancer-agent versions.',
    '',
    '{{usage}}',
    '',
    '{{options}}'
].join('\n');

do_list.synopses = ['{{name}} {{cmd}} [OPTIONS]'];
do_list.completionArgtypes = ['none'];

module.exports = do_list;
