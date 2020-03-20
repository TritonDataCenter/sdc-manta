/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * `mantav2-migrate snaplink-cleanup ...`
 *
 * Command to help with the Snaplink cleanup required as part of migrating
 * to mantav2.
 */

var format = require('util').format;
var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var cmdln = require('cmdln');
var vasync = require('vasync');
var VError = require('verror').VError;

var clicommon = require('./clicommon');

var promptYesNo = clicommon.promptYesNo;

var DBDIR = '/var/db/snaplink-cleanup';

function do_snaplink_cleanup(subcmd, opts, args, cb) {
    var log = this.log;
    var migrator = this.migrator;
    var ui = this.ui;
    var thisDcName = migrator.config.datacenter_name;

    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    } else if (args.length !== 0) {
        cb(new cmdln.UsageError('unexpected arguments'));
        return;
    }

    function saveProgress(progress, next) {
        log.info({progress: progress}, 'save progress');
        migrator.setMantaMetadata(
            {
                SNAPLINK_CLEANUP_PROGRESS: JSON.stringify(progress)
            },
            next
        );
    }

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function getMantaApp(ctx, next) {
                    migrator.getMantaApp(function onMantaApp(err, mantaApp) {
                        if (err) {
                            next(err);
                        } else if (!mantaApp) {
                            ui.info(
                                'No Snaplink cleanup is required (there is ' +
                                    'no "manta" SAPI application)'
                            );
                            next(true);
                        } else {
                            ctx.mantaApp = mantaApp;
                            next();
                        }
                    });
                },

                function checkRequiredMetadatum(ctx, next) {
                    // If SNAPLINK_CLEANUP_REQUIRED is not set on the SAPI app, then
                    // we are done.
                    let required =
                        ctx.mantaApp.metadata.SNAPLINK_CLEANUP_REQUIRED;
                    log.debug('SNAPLINK_CLEANUP_REQUIRED = %j', required);
                    switch (required) {
                        case undefined:
                            ui.info(
                                'No Snaplink cleanup is required ' +
                                    '(SNAPLINK_CLEANUP_REQUIRED metadatum is not set).'
                            );
                            next(true);
                            break;
                        case true:
                            next();
                            break;
                        default:
                            next(
                                new VError(
                                    'invalid value for SNAPLINK_CLEANUP_REQUIRED on ' +
                                        'SAPI "manta" app (%s): it must be the boolean ' +
                                        '`true` or not set, found %s',
                                    ctx.mantaApp,
                                    JSON.stringify(required)
                                )
                            );
                            break;
                    }
                },

                function getSetProgressMetadatum(ctx, next) {
                    // We use the SNAPLINK_CLEANUP_PROGRESS metadatum to coordinate
                    // between possibly multiple DCs in this region. This is stored
                    // on metadata as a JSON string. The structure is
                    //      {
                    //          "infoFromDc": {
                    //              "$dc1Name": {...},
                    //              "$dc2Name": {...},
                    //              ...
                    //          }
                    //      }
                    let progStr =
                        ctx.mantaApp.metadata.SNAPLINK_CLEANUP_PROGRESS;
                    if (!progStr) {
                        // Determine DCs in the region and set progress metadatum.
                        ui.info(
                            'Finding DCs in this region with Manta instances.'
                        );
                        migrator.listDcs(function onDcs(err, dcs) {
                            if (err) {
                                next(err);
                                return;
                            }

                            ctx.progress = {infoFromDc: {}};
                            for (let dc of dcs) {
                                ctx.progress.infoFromDc[dc] = {};
                            }
                            if (!ctx.progress.infoFromDc[thisDcName]) {
                                // Make sure to include the current DC, even if no
                                // Manta instances are deployed here.
                                ctx.progress.infoFromDc[thisDcName] = {};
                            }

                            saveProgress(ctx.progress, next);
                        });
                    } else {
                        try {
                            ctx.progress = JSON.parse(progStr);
                            assert.object(
                                ctx.progress.infoFromDc,
                                'infoFromDc'
                            );
                            assert.ok(
                                Object.keys(ctx.progress.infoFromDc).length > 0,
                                'infoFromDc has entries'
                            );
                            assert.optionalString(
                                ctx.progress.driverDc,
                                'driverDc'
                            );
                        } catch (validateErr) {
                            next(
                                new VError(
                                    validateErr,
                                    '"SNAPLINK_CLEANUP_PROGRESS" metadatum (%s) ' +
                                        'on "manta" SAPI app (%s) is invalid. ' +
                                        'This must be resolved manually before ' +
                                        'snaplink-cleanup can proceed.',
                                    progStr,
                                    ctx.mantaApp.uuid
                                )
                            );
                            return;
                        }
                        next();
                    }
                },

                // Before we can safely clean snaplinks from a Manta region, we
                // need to ensure that no new ones will be created. That is done
                // by ensuring all webapi instances are updated to V2 (where
                // support for creating snaplinks was dropped).
                //
                // The relevant manta-muskie.git commit is
                //
                //    commit 212f935fa661cc1406d38a8031e683500112c104
                //    Date:   2019-12-05T12:46:17-08:00
                //        MANTA-4717 Remove support for creating new SnapLinks (#32)
                //
                // However a commit soon after added the "Server: Manta/2" header
                // we can use to check each webapi instance:
                //
                //    commit 9a6f202d1f4493d564c9f916fa88e3cf7669a9d2
                //    Date:   2019-12-09T12:30:15-08:00
                //        MANTA-4839 change webapi and buckets-api Server header to "Manta/2" (#43)
                //
                // We coordinate across multiple DCs in the same region via
                // the `webapiAtV2` param on `SNAPLINK_CLEANUP_PROGRESS`.
                function ensureWebapiInstsAtV2(ctx, next) {
                    log.debug(
                        {SNAPLINK_CLEANUP_PROGRESS: ctx.progress},
                        'progress'
                    );
                    let tasks = [];

                    for (let dc of Object.keys(ctx.progress.infoFromDc)) {
                        let info = ctx.progress.infoFromDc[dc];
                        if (
                            info.webapiAtV2 === undefined ||
                            info.webapiAtV2 === false
                        ) {
                            tasks.push({dc: dc, info: info});
                        } else if (info.webapiAtV2 !== true) {
                            next(
                                new VError(
                                    'invalid value for "webapiAtV2", %s, in SNAPLINK_CLEANUP_PROGRESS: %s',
                                    JSON.stringify(info.webapiAtV2),
                                    ctx.progress
                                )
                            );
                            return;
                        }
                    }

                    if (tasks.length === 0) {
                        ui.info(
                            'Phase 1: All webapi instances are running V2.'
                        );
                        next();
                        return;
                    }

                    var msgs = [];
                    vasync.forEachPipeline(
                        {
                            inputs: tasks,
                            func: function doOne(task, nextTask) {
                                if (task.dc !== thisDcName) {
                                    // We can't run or check things in other DCs so
                                    // we just instruct the operator to go run things
                                    // in that DC.
                                    if (task.info.webapiAtV2 === undefined) {
                                        msgs.push(
                                            format(
                                                'You must run "mantav2-migrate ' +
                                                    'snaplink-cleanup" in DC %s to ensure webapi instances are upgraded to V2.',
                                                task.dc
                                            )
                                        );
                                    } else {
                                        msgs.push(
                                            format(
                                                'You must upgrade webapi instances in DC %s to a recent enough V2 image (after 2019-12-09), and then re-run "mantav2-migrate ' +
                                                    'snaplink-cleanup" there to update snaplink-cleanup progress.',
                                                task.dc
                                            )
                                        );
                                    }
                                    nextTask();
                                } else {
                                    ui.info(
                                        'Determining if webapi instances in this DC are at V2.'
                                    );
                                    migrator.getWebapiV2Info(function onInfo(
                                        err,
                                        wInfo
                                    ) {
                                        if (err) {
                                            nextTask(err);
                                            return;
                                        }
                                        let v1Insts = [];
                                        let errInsts = [];
                                        for (let inst of wInfo) {
                                            if (inst.err) {
                                                errInsts.push(inst);
                                            } else if (!inst.isV2) {
                                                v1Insts.push(inst);
                                            }
                                        }
                                        if (
                                            v1Insts.length &&
                                            v1Insts.length === wInfo.length
                                        ) {
                                            msgs.push(
                                                format(
                                                    'You must upgrade all webapi instances (%d) ' +
                                                        'in this DC (%s) to a recent enough V2 image ' +
                                                        '(after 2019-12-09), and then re-run ' +
                                                        '"mantav2-migrate snaplink-cleanup" to ' +
                                                        'update snaplink-cleanup progress.',
                                                    v1Insts.length,
                                                    thisDcName
                                                )
                                            );
                                        } else if (v1Insts.length) {
                                            let summary = v1Insts
                                                .map(inst => inst.uuid)
                                                .join(', ');
                                            msgs.push(
                                                format(
                                                    'You must upgrade the following webapi instances (%d) ' +
                                                        'in this DC (%s) to a recent enough V2 image ' +
                                                        '(after 2019-12-09), and then re-run ' +
                                                        '"mantav2-migrate snaplink-cleanup" to ' +
                                                        'update snaplink-cleanup progress: %s',
                                                    v1Insts.length,
                                                    thisDcName,
                                                    summary
                                                )
                                            );
                                        }
                                        if (errInsts.length) {
                                            let summary = errInsts
                                                .map(inst =>
                                                    format(
                                                        '%s (%s): %s',
                                                        inst.uuid,
                                                        inst.alias,
                                                        inst.err
                                                    )
                                                )
                                                .join('\n    ');
                                            msgs.push(
                                                format(
                                                    'Could not determine if the following %d ' +
                                                        'webapi instance(s) in this DC (%s) are at V2:\n' +
                                                        '    %s',
                                                    errInsts.length,
                                                    thisDcName,
                                                    summary
                                                )
                                            );
                                        }

                                        if (
                                            !v1Insts.length &&
                                            !errInsts.length
                                        ) {
                                            ctx.progress.infoFromDc[
                                                thisDcName
                                            ].webapiAtV2 = true;
                                        } else {
                                            ctx.progress.infoFromDc[
                                                thisDcName
                                            ].webapiAtV2 = false;
                                        }
                                        saveProgress(ctx.progress, nextTask);
                                    });
                                }
                            }
                        },
                        function(err) {
                            if (err) {
                                next(err);
                                return;
                            }

                            if (msgs.length) {
                                ui.info(
                                    [
                                        '',
                                        '',
                                        'Phase 1: Update webapis to V2',
                                        '',
                                        'Snaplinks cannot be fully cleaned until all webapi instances are',
                                        'are updated to a V2 image that no longer allows new snaplinks',
                                        'to be created.',
                                        ''
                                    ].join('\n')
                                );
                                for (let msg of msgs) {
                                    ui.info(clicommon.bullet(msg, 80));
                                }
                                ui.info('');
                                next(
                                    new VError(
                                        'webapi upgrades are required before snaplink-cleanup can proceed'
                                    )
                                );
                            } else {
                                ui.info('All webapi instances are running V2.');
                                next();
                            }
                        }
                    );
                },

                // Subsequent snaplink-cleanup work must coordinate files on a
                // *single* DC in this region. We call this the "driver" DC.
                function checkDriverDc(ctx, next) {
                    var driverDc = ctx.progress.driverDc;
                    if (driverDc === thisDcName) {
                        log.debug({driverDc: driverDc}, 'running in driverDc');
                        ui.info(
                            'Phase 2: Driver DC is "%s" (this one)',
                            driverDc
                        );
                        next();
                    } else if (driverDc) {
                        ui.info('Phase 2: Driver DC is "%s"', driverDc);
                        next(
                            new VError(
                                'continue with "mantav2-migrate snaplink-cleanup" in the driver DC %s',
                                driverDc
                            )
                        );
                    } else if (
                        !driverDc &&
                        Object.keys(ctx.progress.infoFromDc).length === 1
                    ) {
                        log.debug(
                            {driverDc: thisDcName},
                            'set driverDc to this DC because it is the only one'
                        );
                        ctx.progress.driverDc = thisDcName;
                        ui.info(
                            'Phase 2: Driver DC is "%s" (the sole DC)',
                            thisDcName
                        );
                        saveProgress(ctx.progress, next);
                    } else {
                        // Ask operator if want to use this DC as the driverDc.
                        ui.info(
                            [
                                '',
                                '',
                                '# Phase 2: Driver DC',
                                '',
                                'Subsequent runs of "mantav2-migrate snaplink-cleanup" must all be run on the',
                                'same DC in this region. We will call this the "driver DC". Any DC can be used.',
                                'If you would like to use this DC, enter "yes" to the following. Otherwise,',
                                'answer "no" and run this command on the DC you would like to be the driver.',
                                ''
                            ].join('\n')
                        );
                        let msg =
                            'Would you like to use this DC as the driver? [y/N] ';
                        promptYesNo({msg: msg, default: 'n'}, function(answer) {
                            if (answer !== 'y') {
                                ui.info('Aborting.');
                                next(
                                    new VError(
                                        'run "mantav2-migrate snaplink-cleanup" on ' +
                                            'the DC you would like to be the driver'
                                    )
                                );
                                return;
                            }

                            ui.info('');
                            ctx.progress.driverDc = thisDcName;
                            saveProgress(ctx.progress, next);
                        });
                    }
                },

                function mkDbDir(_, next) {
                    var skelDirs = [DBDIR, path.join(DBDIR, 'discovery')];
                    vasync.forEachPipeline(
                        {
                            inputs: skelDirs,
                            func: function mkOneDir(dir, nextDir) {
                                fs.mkdir(dir, function onMkdir(err) {
                                    if (err && err.code !== 'EEXIST') {
                                        nextDir(err);
                                    } else {
                                        nextDir();
                                    }
                                });
                            }
                        },
                        next
                    );
                },

                // The "discovery" phase is where we guide the operator to run a
                // tool (snaplink-sherlock.sh) against every Manta index shard
                // to list all snaplinks. This tool needs to do a linear scan of
                // the full manta table, so can take a long time. The list of
                // found snaplinks are written to a file.
                //
                // Those files, one from each index shard, must be copied back
                // to a given dir on the driver DC headnode for this phase to be
                // considered complete.
                function discoveryPhase(ctx, next) {
                    let region = ctx.mantaApp.metadata.REGION;
                    let domainName = '.' + ctx.mantaApp.metadata.DOMAIN_NAME;
                    ctx.indexShards = [];
                    ctx.missingShards = [];

                    for (let s of ctx.mantaApp.metadata.INDEX_MORAY_SHARDS) {
                        assert(
                            s.host.endsWith(domainName),
                            `index shard "${s.host}" does not end with "${domainName}"`
                        );
                        let shard = {
                            host: s.host,
                            name: s.host.slice(0, -domainName.length)
                        };
                        shard.discoveryFile = path.join(
                            DBDIR,
                            'discovery',
                            `${shard.host}_sherlock.tsv.gz`
                        );
                        shard.discoveryFileExists = fs.existsSync(
                            shard.discoveryFile
                        );
                        ctx.indexShards.push(shard);
                        if (!shard.discoveryFileExists) {
                            ctx.missingShards.push(shard);
                        }
                    }
                    log.debug(
                        {indexShards: ctx.indexShards},
                        'manta index shards info'
                    );

                    if (ctx.missingShards.length === 0) {
                        ui.info(
                            'Phase 3: Have snaplink listings for all (%d) Manta index shards.',
                            ctx.indexShards.length
                        );
                        next();
                        return;
                    }

                    ui.info(
                        [
                            '',
                            '',
                            '# Phase 3: Discovery',
                            '',
                            'In this phase, you must run the "snaplink-sherlock.sh" tool against',
                            'the async postgres for each Manta index shard. That will generate a',
                            `"{shard}_sherlock.tsv.gz" file that must be copied back to`,
                            `"${DBDIR}/discovery/" on this headnode.`,
                            '',
                            'Repeat the following steps for each missing shard:',
                            // XXX to master branch for merge
                            '    https://github.com/joyent/manta/blob/MANTA-4874/docs/operator-guide/mantav2-migration.md#snaplink-discovery',
                            '',
                            `Missing "*_sherlock.tsv.gz" for the following shards (${ctx.missingShards.length} of ${ctx.indexShards.length}):`
                        ].join('\n')
                    );
                    for (let s of ctx.missingShards) {
                        ui.info(`    ${s.host}`);
                    }
                    ui.info('');

                    next(
                        new VError(
                            'sherlock files must be generated and ' +
                                'copied to "%s/discovery/" before snaplink cleanup ' +
                                'can proceed',
                            DBDIR
                        )
                    );
                },

                function generateDelinkScripts(ctx, next) {
                    migrator.ensureDelinkScripts(
                        {
                            dbDir: DBDIR,
                            indexShards: ctx.indexShards
                        },
                        function(err, scripts, generated) {
                            if (err) {
                                next(err);
                                return;
                            }

                            ctx.delinkScripts = scripts;
                            if (generated) {
                                ui.info(
                                    'Created delink scripts in %s/delink/',
                                    DBDIR
                                );
                                ui.info('  stordelink scripts:')
                                for (let script of scripts.stordelink) {
                                    ui.info('    ' + path.basename(script));
                                }
                                ui.info('  moraydelink scripts:')
                                for (let script of scripts.moraydelink) {
                                    ui.info('    ' + path.basename(script));
                                }
                            }
                            next();
                        }
                    );
                },

                function confirmDelinkScriptsHaveBeenRun(ctx, next) {
                    ui.info(
                        [
                            '',
                            '',
                            '# Phase 4: Running delink scripts',
                            '',
                            '"Delink" scripts have been generated from the snaplink listings',
                            'from the previous phase. In this phase, you must:',
                            '',
                            '1. Copy each "*_stordelink.sh" script to the appropriate storage',
                            '   node and run it there. There are %d to run:',
                            '',
                            '        # {storage_id}_stordelink.sh',
                            '        ls %s/delink/*_stordelink.sh',
                            '',
                            '   Use the following to help locate each storage node:',
                            '',
                            '        manta-adm show -a -o service,storage_id,datacenter,zonename,gz_host,gz_admin_ip | grep ^storage',
                            '',
                            '2. **Only after** all stordelink scripts have been run, copy',
                            '   each "*_moraydelink.sh" script to a moray zone for the',
                            '   appropriate shard and run it there. There is one script',
                            '   script for each Manta index shard (%d):',
                            '',
                            '        # {shard}_moraydelink.sh',
                            '        ls %s/delink/*_moraydelink.sh',
                            '',
                            '   Use the following to help locate a moray for each shard:',
                            '',
                            '        manta-adm show -o service,shard,zonename,gz_host,gz_admin_ip | grep ^moray',
                            '',
                            'When you are sure you have run all these scripts, then answer',
                            'the following to proceed. *WARNING* Be sure you have run all',
                            'these scripts successfully, otherwise lingering snaplinks in the',
                            'system can cause the garbage-collector and rebalancer systems',
                            'to lose data.',
                            ''
                        ].join('\n'),
                        ctx.delinkScripts.stordelink.length,
                        DBDIR,
                        ctx.delinkScripts.moraydelink.length,
                        DBDIR
                    );

                    clicommon.promptConfirm(
                        {
                            confirmationStr: 'delinked',
                            msg:
                                'Enter "delinked" when all delink scripts have been successfully run: '
                        },
                        function(confirmed) {
                            if (confirmed) {
                                next();
                            } else {
                                ui.info(
                                    'Aborting. Re-run this command when ' +
                                        'all delink scripts have been run.'
                                );
                                next(
                                    new VError(
                                        'delink scripts must be run before snaplink ' +
                                            'cleanup can proceed'
                                    )
                                );
                            }
                        }
                    );
                },

                function removeSnaplinkCleanupMetadata(_, next) {
                    ui.info('Removing "SNAPLINK_CLEANUP_REQUIRED" metadatum.');
                    migrator.deleteMantaMetadata(
                        [
                            'SNAPLINK_CLEANUP_REQUIRED',
                            'SNAPLINK_CLEANUP_PROGRESS'
                        ],
                        function(err) {
                            if (err) {
                                next(err);
                            } else {
                                ui.info('All snaplinks have been removed!');
                                next();
                            }
                        }
                    );
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

do_snaplink_cleanup.options = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Show this help.'
    }
];

do_snaplink_cleanup.help = [
    'Clean out Snaplinks from this Manta region.',
    '',
    '{{usage}}',
    '',
    '{{options}}',
    'Run this command and following the given instructions to work through',
    'cleaning all snaplinks from this Manta. The process will involve',
    'running this command *multiple times* and *on each DC in this Manta',
    'region*.',
    '',
    'Warning: Progress is shared across DCs in the "manta" SAPI app metadata',
    '(the SNAPLINK_CLEANUP_PROGRESS var). Please do *not* run this command',
    'concurrently in multiple DCs to avoid colliding updates to that progress.',
    '',
    'Broadly the snaplink process involves the following phases:',
    '',
    ' - Ensure all "webapi" instances are upgraded to mantav2-webapi image',
    '   that no longer allows new snaplink creation (images published after',
    '   2019-12-09).',
    ' - Discovery phase. Run a "snapshot-sherlock.sh" script against the',
    '   async manatee of every Manta index shard to get a listing of all',
    '   snaplinks. This involves a linear scan of the "manta" table, so can',
    '   take a long time. The snaplink listing files are all collected.',
    ' - Delink phase. The snaplink listing files from the previous phase are',
    '   used to create a set of "delinking" scripts to be run (a) on each',
    '   storage node (to create a separate new object file for each snaplink),',
    "   and (b) on each shard's moray to update object metadata accordingly."
].join('\n');

do_snaplink_cleanup.synopses = ['{{name}} {{cmd}} [OPTIONS]'];
do_snaplink_cleanup.completionArgtypes = ['none'];

module.exports = do_snaplink_cleanup;
