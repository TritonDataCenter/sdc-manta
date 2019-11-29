/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.maint_windows.js: integration test for maintenance windows.  This test
 * exercises the "manta-adm alarm maint" functionality, including:
 *
 * - a bunch of invalid invocations
 * - creating a few windows
 * - listing and showing details about these windows
 * - removing these windows
 */

var assertplus = require('assert-plus');
var cmdutil = require('cmdutil');
var forkexec = require('forkexec');
var path = require('path');
var strsplit = require('strsplit');
var vasync = require('vasync');
var VError = require('verror');

/*
 * Below are parameters used for windows created by this test.  The "example"
 * values are the ones we use as a base for most successful cases.  These dates
 * are deliberately very far in the future so that Amon will not remove them
 * while we're running.  To those debugging this test failing on the first
 * Monday (US time) in 2038: sorry for this, and best of luck with that Y2038
 * thing.
 */
var exampleStart = '2038-01-06T17:00:00Z';
var exampleEnd = '2038-01-06T21:00:00Z';
var exampleNote = 'tst.maint_windows.js test';
var exampleArgs = [
    'create',
    '--start',
    exampleStart,
    '--end',
    exampleEnd,
    '--notes',
    exampleNote
];

/* "end" time for a window longer than the expected maximum */
var longEnd = '2038-01-06T21:01:00Z';
/* "start" and "end" times for a window in the past */
var pastStart = '2007-07-16T00:00:00Z';
var pastEnd = '2017-07-16T01:00:00Z';

/* path to the "manta-adm" executable */
var execname = path.join(__dirname, '..', '..', 'bin', 'manta-adm');
/* initial arguments used for all command invocations */
var baseArgs = [process.execPath, execname, 'alarm', 'maint'];

/*
 * These counters help verify that the Node program does not exit prematurely.
 */
var nstarted = 0;
var ndone = 0;

var testsInvalid;

function main() {
    vasync.forEachPipeline(
        {
            func: runTestCase,
            inputs: testsInvalid
        },
        function(err) {
            if (err) {
                cmdutil.fail(err);
            }

            assertplus.equal(ndone, nstarted);
            assertplus.equal(nstarted, testsInvalid.length);

            var ctx = {};
            nstarted++;
            vasync.pipeline(
                {
                    arg: ctx,
                    funcs: validPipeline
                },
                function(pipelineErr) {
                    if (pipelineErr) {
                        cmdutil.fail(pipelineErr);
                    }

                    assertplus.equal(ndone, nstarted);
                }
            );
        }
    );
}

/*
 * Each of these test cases should cause the command to exit with non-zero
 * status and an error message.  These should have no side effects and do not
 * require that any Triton services be configured or online.
 */
testsInvalid = [
    {
        name: 'missing command',
        argv: [],
        error: /^manta-adm alarm: error: no command given$/
    },
    {
        name: 'create: --start: missing',
        argv: ['create', '--end', exampleEnd, '--notes', exampleNote],
        error: /^manta-adm alarm: error: argument is required: --start$/
    },
    {
        name: 'create: --end: missing',
        argv: ['create', '--start', exampleStart, '--notes', exampleNote],
        error: /^manta-adm alarm: error: argument is required: --end$/
    },
    {
        name: 'create: --notes: missing',
        argv: ['create', '--start', exampleStart, '--end', exampleEnd],
        error: /^manta-adm alarm: error: argument is required: --notes$/
    },
    {
        name: 'create: --start: bad date',
        argv: [
            'create',
            '--start',
            'never',
            '--end',
            exampleEnd,
            '--notes',
            exampleNote
        ],
        error: /^manta-adm alarm: error: unsupported value for --start: never$/
    },
    {
        name: 'create: --end: bad date',
        argv: [
            'create',
            '--start',
            exampleStart,
            '--end',
            'never',
            '--notes',
            exampleNote
        ],
        error: /arg for "--end" is not a valid date format: "never"$/
    },
    {
        name: 'create: --start/--end: entire window is in the past',
        argv: [
            'create',
            '--start',
            pastStart,
            '--end',
            pastEnd,
            '--notes',
            exampleNote
        ],
        error: /^manta-adm alarm: error: cannot create windows in the past$/
    },
    {
        name: 'create: --start/--end: window is empty',
        argv: [
            'create',
            '--start',
            exampleStart,
            '--end',
            exampleStart,
            '--notes',
            exampleNote
        ],
        error: /specified window does not start before it ends/
    },
    {
        name: 'create: bad combination of scopes (probe, probe group)',
        argv: exampleArgs.concat([
            '--probe',
            'probe1',
            '--probegroup',
            'group1'
        ]),
        error: /only one of --probe, --probegroup, or --machine/
    },
    {
        name: 'create: bad combination of scopes (probe, machine)',
        argv: exampleArgs.concat([
            '--probe',
            'probe1',
            '--machine',
            'machine1'
        ]),
        error: /only one of --probe, --probegroup, or --machine/
    },
    {
        name: 'create: bad combination of scopes (probe group, machine)',
        argv: exampleArgs.concat([
            '--probegroup',
            'group1',
            '--machine',
            'machine1'
        ]),
        error: /only one of --probe, --probegroup, or --machine/
    },
    {
        name: 'create: --probe: bad value',
        argv: exampleArgs.concat(['--probe', 'foo,bar']),
        error: /identifier "foo,bar": does not look like a valid uuid$/
    },
    {
        name: 'create: --probegroup: bad value',
        argv: exampleArgs.concat(['--probegroup', 'foo,bar']),
        error: /identifier "foo,bar": does not look like a valid uuid$/
    },
    {
        name: 'create: --machine: bad value',
        argv: exampleArgs.concat(['--machine', 'foo,bar']),
        error: /identifier "foo,bar": does not look like a valid uuid$/
    },
    {
        name: 'create: extra arguments',
        argv: exampleArgs.concat(['boom']),
        error: /^manta-adm alarm: error: unexpected arguments$/
    },
    {
        name: 'delete: missing arguments',
        argv: ['delete'],
        error: /^manta-adm alarm: error: expected WINID$/
    },
    {
        name: 'delete: invalid concurrency',
        argv: ['delete', '--concurrency=bump'],
        error: /arg for "--concurrency" is not a positive integer: "bump"$/
    },
    {
        name: 'list: extra arguments',
        argv: ['list', 'boom'],
        error: /^manta-adm alarm: error: unexpected arguments$/
    },
    {
        name: 'show: extra arguments',
        argv: ['show', 'boom'],
        error: /^manta-adm alarm: error: unexpected arguments$/
    }
];

function runTestCase(tc, callback) {
    var argv;

    assertplus.object(tc, 'tc');
    assertplus.string(tc.name, 'tc.name');
    assertplus.arrayOfString(tc.argv, 'tc.argv');

    console.error('test case: %s', tc.name);
    argv = baseArgs.concat(tc.argv);
    nstarted++;
    forkexec.forkExecWait(
        {
            argv: argv
        },
        function(err, result) {
            var stderr, testresult;

            ndone++;
            assertplus.number(result.status, 'command did not exit normally');
            assertplus.strictEqual(
                result.signal,
                null,
                'command did not exit normally'
            );

            if (tc.error) {
                assertplus.ok(
                    result.status !== 0,
                    'expected non-zero exit status'
                );
                stderr = result.stderr.trim();
                testresult = tc.error.test(stderr);
                if (!testresult) {
                    console.error('output did not match expected');
                    console.error('found:    %s', stderr);
                    console.error('expected: %s', tc.error.source);
                    callback(new Error('test case failed'));
                    return;
                }
            } else {
                assertplus.strictEqual(
                    result.status,
                    0,
                    'expected zero exit status'
                );
            }

            callback(null, result);
        }
    );
}

function findTestWindows(callback) {
    var found;

    console.error('listing windows to find test windows');
    found = [];
    forkexec.forkExecWait(
        {
            argv: baseArgs.concat(['list', '-H', '-o', 'win,notes'])
        },
        function(err, result) {
            var windows;

            if (err) {
                callback(new VError(err, 'listing for test windows'));
                return;
            }

            windows = result.stdout.trim().split('\n');
            windows.forEach(function(l) {
                var parts = strsplit(l.trim(), /\s+/, 2);
                if (parts[1] === exampleNote) {
                    console.error(
                        '    found window %s from this test suite',
                        parts[0]
                    );
                    found.push(parts[0]);
                } else {
                    console.error(
                        '    ignoring pre-existing window %s',
                        parts[0]
                    );
                }
            });

            callback(null, found);
        }
    );
}

/*
 * The following executes a valid sequence of commands that show, create, list,
 * and delete maintenance windows.  If this fails partway through, it may leave
 * these windows around.  However, upon startup and any successful completion,
 * it removes any windows created by previous invocations.  It does not touch
 * windows that existed prior to the test starting.
 *
 * The valid sequence looks like this:
 *
 * - find and remove windows from previous tests
 * - initial "list" and "show", used to compare against later output
 * - run through several normal "create" test cases
 * - "list" and "show" the newly-created windows
 * - run through "create" cases that produce warnings
 * - "delete" one of the new windows, also supplying arguments to the "delete"
 *   that produce operational errors.
 * - "list" again to show the window was deleted
 * - "delete" the rest of the windows we created (exiting status 0)
 * - final "list" and "show" should match initial output
 */
var validPipeline = [
    /*
     * Find and remove windows from previous invocations of this command.
     */

    function listOldWindows(ctx, callback) {
        findTestWindows(function(err, found) {
            if (err) {
                callback(err);
                return;
            }

            ctx.ctx_found = found;
            callback();
        });
    },

    function removeOldTestWindows(ctx, callback) {
        if (ctx.ctx_found.length === 0) {
            console.error('no old windows to remove');
            setImmediate(callback);
            return;
        }

        console.error(
            'removing %s old window%s',
            ctx.ctx_found.length,
            ctx.ctx_found.length != 1 ? 's' : ''
        );
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['delete']).concat(ctx.ctx_found)
            },
            function(err) {
                if (err) {
                    err = new VError(err, 'removing old windows');
                }

                callback(err);
            }
        );
    },

    /*
     * Save the output of "show" and "list" for comparison later.
     */

    function showInitial(ctx, callback) {
        console.error('initial "show"');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['show'])
            },
            function(err, result) {
                if (err) {
                    callback(new VError(err, 'initial "show"'));
                    return;
                }

                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                ctx.ctx_initial_show = result.stdout;
                callback();
            }
        );
    },

    function listInitial(ctx, callback) {
        console.error('initial "list"');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['list'])
            },
            function(err, result) {
                if (err) {
                    callback(new VError(err, 'initial "list"'));
                    return;
                }

                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                ctx.ctx_initial_list = result.stdout;
                callback();
            }
        );
    },

    /*
     * Run through several "create" test cases.
     */

    function createNormalAll(ctx, callback) {
        runTestCase(
            {
                name: 'normal create, scope "all"',
                argv: exampleArgs
            },
            function(err, result) {
                assertplus.ok(!err);
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                callback();
            }
        );
    },

    function createNormalMachines(ctx, callback) {
        runTestCase(
            {
                name: 'normal create, scope "machines"',
                argv: exampleArgs.concat(['--machine', 'machine1'])
            },
            function(err, result) {
                assertplus.ok(!err);
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                callback();
            }
        );
    },

    function createNormalProbes(ctx, callback) {
        runTestCase(
            {
                name: 'normal create, scope "probes"',
                argv: exampleArgs.concat([
                    '--probe',
                    'probe1',
                    '--probe',
                    'probe2'
                ])
            },
            function(err, result) {
                assertplus.ok(!err);
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                callback();
            }
        );
    },

    function createNormalGroups(ctx, callback) {
        runTestCase(
            {
                name: 'normal create, scope "probegroups"',
                argv: exampleArgs.concat(['--probegroup', 'group1'])
            },
            function(err, result) {
                assertplus.ok(!err);
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                callback();
            }
        );
    },

    /*
     * At this point, list the newly created windows.  This is a basic
     * exercise of the "list" command output.  Because all of the windows
     * that we've created up to this point are in the far future, we expect
     * the initial "list" output to be a prefix of this new output.
     */

    function listNew(ctx, callback) {
        console.error('list all windows');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['list'])
            },
            function(err, result) {
                var lines;

                if (err) {
                    callback(new VError(err, 'later "list"'));
                    return;
                }

                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                assertplus.ok(
                    result.stdout.indexOf(ctx.ctx_initial_list) === 0
                );

                /*
                 * Strip out the unique identifiers from the
                 * newly-created windows so that we can have stable
                 * output for a basic stdout comparison.  Because the
                 * expected stdout is supposed to be hand-checked
                 * whenever it's updated, if this regexp becomes
                 * erroneously aggressive, that should cause the test to
                 * fail.
                 */
                console.log('"list" for newly-created windows:');
                lines = result.stdout
                    .substr(ctx.ctx_initial_list.length)
                    .trim()
                    .split('\n');
                lines.forEach(function(l) {
                    console.log(
                        '%s',
                        l.replace(/^\s*\d+/, '<id_stripped_by_test_suite>')
                    );
                });
                callback();
            }
        );
    },

    function showNew(ctx, callback) {
        console.error('show all windows');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['show'])
            },
            function(err, result) {
                var lines;

                if (err) {
                    callback(new VError(err, 'later "show"'));
                    return;
                }

                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                assertplus.ok(
                    result.stdout.indexOf(ctx.ctx_initial_show) === 0
                );

                /*
                 * See the note about "list" above.
                 */
                console.log('"show" for newly-created windows:');
                lines = result.stdout
                    .substr(ctx.ctx_initial_show.length)
                    .trim()
                    .split('\n');
                lines.forEach(function(l) {
                    console.log(
                        '%s',
                        l.replace(
                            /^MAINTENANCE WINDOW\s*\d+:/,
                            'MAINTENANCE WINDOW ' +
                                '<id_stripped_by_test_suite>:'
                        )
                    );
                });

                callback();
            }
        );
    },

    /*
     * Exercise "create" cases that produce warnings: a long window, and a
     * window that extends into the past.
     */

    function createLong(ctx, callback) {
        runTestCase(
            {
                name: 'create long window',
                argv: [
                    'create',
                    '--start',
                    exampleStart,
                    '--end',
                    longEnd,
                    '--notes',
                    exampleNote
                ]
            },
            function(err, result) {
                var warnings;

                assertplus.ok(!err);
                warnings = result.stderr
                    .trim()
                    .split('\n')
                    .sort();
                assertplus.deepEqual(warnings, [
                    'note: maintenance window exceeds expected ' +
                        'maximum (4h00m00s)'
                ]);
                callback();
            }
        );
    },

    function createPartwayPast(ctx, callback) {
        runTestCase(
            {
                name: 'create window extending into past',
                argv: [
                    'create',
                    '--start',
                    pastStart,
                    '--end',
                    exampleEnd,
                    '--notes',
                    exampleNote
                ]
            },
            function(err, result) {
                var warnings;

                assertplus.ok(!err);
                warnings = result.stderr
                    .trim()
                    .split('\n')
                    .sort();
                assertplus.deepEqual(warnings, [
                    'note: maintenance window exceeds expected ' +
                        'maximum (4h00m00s)',
                    'note: maintenance window starts in the past'
                ]);
                callback();
            }
        );
    },

    /*
     * Find and delete one of the windows we created, plus a few invalid
     * values.  This should successfully delete the one we created and split
     * out warnings for the other values.
     */

    function findNewTestWindows(ctx, callback) {
        findTestWindows(function(err, found) {
            if (err) {
                callback(err);
                return;
            }

            assertplus.equal(
                found.length,
                6,
                'expected six test windows created so far'
            );
            ctx.ctx_new = found;
            callback();
        });
    },

    function deleteValidAndInvalid(ctx, callback) {
        console.error('removing one valid and several invalid windows');
        forkexec.forkExecWait(
            {
                argv: baseArgs
                    .concat(['delete'])
                    .concat(['--', 'bogus', ctx.ctx_new[0], '-1'])
            },
            function(err, result) {
                var errors;

                errors = result.stderr
                    .trim()
                    .split('\n')
                    .sort();
                assertplus.notStrictEqual(result.status, 0);
                assertplus.deepEqual(errors, [
                    'error: window "-1": not a positive integer',
                    'error: window "bogus": invalid number: "bogus"'
                ]);
                callback();
            }
        );
    },

    /*
     * Prove to ourselves that despite those errors, one window was removed.
     */
    function findRemainingTestWindows(ctx, callback) {
        findTestWindows(function(err, found) {
            if (err) {
                callback(err);
                return;
            }

            assertplus.equal(
                found.length,
                5,
                'expected five test windows remaining'
            );
            assertplus.deepEqual(found, ctx.ctx_new.slice(1));
            ctx.ctx_remaining = found;
            callback();
        });
    },

    /*
     * Remove the rest of the windows that we've created.
     */
    function deleteRemainingTestWindows(ctx, callback) {
        console.error('remove remaining test windows');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['delete']).concat(ctx.ctx_remaining)
            },
            function(err, result) {
                assertplus.strictEqual(result.status, 0);
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                callback();
            }
        );
    },

    /*
     * Finally, "list" and "show" again and make sure the output is the same
     * as before we started.
     */

    function checkFinalList(ctx, callback) {
        console.error('list final');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['list'])
            },
            function(err, result) {
                if (err) {
                    callback(new VError(err, 'final "list"'));
                    return;
                }

                assertplus.strictEqual(
                    result.stdout,
                    ctx.ctx_initial_list,
                    'unexpected change from start'
                );
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                callback();
            }
        );
    },

    function checkFinalShow(ctx, callback) {
        console.error('show final');
        forkexec.forkExecWait(
            {
                argv: baseArgs.concat(['show'])
            },
            function(err, result) {
                if (err) {
                    callback(new VError(err, 'final "show"'));
                    return;
                }

                assertplus.strictEqual(
                    result.stdout,
                    ctx.ctx_initial_show,
                    'unexpected change from start'
                );
                assertplus.strictEqual(
                    result.stderr,
                    '',
                    'unexpected content on stderr'
                );
                ndone++;
                callback();
            }
        );
    }
];

main();
