/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var tty = require('tty');

var assert = require('assert-plus');
var strsplit = require('strsplit');

function objCopy(obj, target) {
    if (!target) {
        target = {};
    }
    Object.keys(obj).forEach(function(k) {
        target[k] = obj[k];
    });
    return target;
}

/**
 * Prompt a user for a y/n answer.
 *
 *      cb('y')        user entered in the affirmative
 *      cb('n')        user entered in the negative
 *      cb(false)      user ^C'd
 */
function promptYesNo(opts_, cb) {
    assert.object(opts_, 'opts');
    assert.string(opts_.msg, 'opts.msg');
    assert.optionalString(opts_.default, 'opts.default');
    var opts = objCopy(opts_);

    // Setup stdout and stdin to talk to the controlling terminal if
    // process.stdout or process.stdin is not a TTY.
    var stdout;
    if (opts.stdout) {
        stdout = opts.stdout;
    } else if (process.stdout.isTTY) {
        stdout = process.stdout;
    } else {
        opts.stdout_fd = fs.openSync('/dev/tty', 'r+');
        stdout = opts.stdout = new tty.WriteStream(opts.stdout_fd);
    }
    var stdin;
    if (opts.stdin) {
        stdin = opts.stdin;
    } else if (process.stdin.isTTY) {
        stdin = process.stdin;
    } else {
        opts.stdin_fd = fs.openSync('/dev/tty', 'r+');
        stdin = opts.stdin = new tty.ReadStream(opts.stdin_fd);
    }

    stdout.write(opts.msg);
    stdin.setEncoding('utf8');
    stdin.setRawMode(true);
    stdin.resume();
    var input = '';
    stdin.on('data', onData);

    function postInput() {
        stdout.write('\n');
        stdin.setRawMode(false);
        stdin.pause();
        stdin.removeListener('data', onData);
    }

    function finish(rv) {
        if (opts.stdout_fd !== undefined) {
            stdout.end();
            delete opts.stdout_fd;
        }
        if (opts.stdin_fd !== undefined) {
            stdin.end();
            delete opts.stdin_fd;
        }
        cb(rv);
    }

    function onData(ch) {
        ch = ch + '';

        switch (ch) {
            case '\n':
            case '\r':
            case '\u0004':
                // They've finished typing their answer
                postInput();
                var answer = input.toLowerCase();
                if (answer === '' && opts.default) {
                    finish(opts.default);
                } else if (answer === 'yes' || answer === 'y') {
                    finish('y');
                } else if (answer === 'no' || answer === 'n') {
                    finish('n');
                } else {
                    stdout.write('Please enter "y", "yes", "n" or "no".\n');
                    promptYesNo(opts, cb);
                    return;
                }
                break;
            case '\u0003':
                // Ctrl C
                postInput();
                finish(false);
                break;
            case '\u007f': // DEL
                input = input.slice(0, -1);
                stdout.clearLine();
                stdout.cursorTo(0);
                stdout.write(opts.msg);
                stdout.write(input);
                break;
            default:
                // More plaintext characters
                stdout.write(ch);
                input += ch;
                break;
        }
    }
}

/**
 * Interactively prompt the user for a confirmation string.
 *
 *      cb(true)       The user entered the confirmation string.
 *      cb(false)      The user ^C'd.
 */
function promptConfirm(opts_, cb) {
    assert.object(opts_, 'opts');
    assert.string(opts_.msg, 'opts.msg');
    assert.string(opts_.confirmationStr, 'opts.confirmationStr');
    var opts = objCopy(opts_);

    // Setup stdout and stdin to talk to the controlling terminal if
    // process.stdout or process.stdin is not a TTY.
    var stdout;
    if (opts.stdout) {
        stdout = opts.stdout;
    } else if (process.stdout.isTTY) {
        stdout = process.stdout;
    } else {
        opts.stdout_fd = fs.openSync('/dev/tty', 'r+');
        stdout = opts.stdout = new tty.WriteStream(opts.stdout_fd);
    }
    var stdin;
    if (opts.stdin) {
        stdin = opts.stdin;
    } else if (process.stdin.isTTY) {
        stdin = process.stdin;
    } else {
        opts.stdin_fd = fs.openSync('/dev/tty', 'r+');
        stdin = opts.stdin = new tty.ReadStream(opts.stdin_fd);
    }

    stdout.write(opts.msg);
    stdin.setEncoding('utf8');
    stdin.setRawMode(true);
    stdin.resume();
    var input = '';
    stdin.on('data', onData);

    function postInput() {
        stdout.write('\n');
        stdin.setRawMode(false);
        stdin.pause();
        stdin.removeListener('data', onData);
    }

    function finish(rv) {
        if (opts.stdout_fd !== undefined) {
            stdout.end();
            delete opts.stdout_fd;
        }
        if (opts.stdin_fd !== undefined) {
            stdin.end();
            delete opts.stdin_fd;
        }
        cb(rv);
    }

    function onData(ch) {
        ch = ch + '';

        switch (ch) {
            case '\n':
            case '\r':
            case '\u0004':
                // They've finished typing their answer
                postInput();
                var answer = input.toLowerCase();
                if (answer === opts.confirmationStr) {
                    finish(true);
                } else {
                    stdout.write(
                        `Please enter "${opts.confirmationStr}" to ` +
                            'confirm, or Ctrl+C to abort.\n\n'
                    );
                    promptConfirm(opts, cb);
                    return;
                }
                break;
            case '\u0003':
                // Ctrl C
                postInput();
                finish(false);
                break;
            case '\u007f': // DEL
                input = input.slice(0, -1);
                stdout.clearLine();
                stdout.cursorTo(0);
                stdout.write(opts.msg);
                stdout.write(input);
                break;
            default:
                // More plaintext characters
                stdout.write(ch);
                input += ch;
                break;
        }
    }
}

function wrappedLinesFromText(text, len) {
    let words = text.split(/\s+/g);
    let lines = [];
    let line = '';
    for (let w of words) {
        if (line.length + 1 + w.length > len) {
            lines.push(line);
            line = '';
        }
        if (!line) {
            line = w;
        } else {
            line += ' ' + w;
        }
    }
    if (line) {
        lines.push(line);
    }
    return lines;
}

// Given a string, return a text bullet list item that wraps at `len` chars or
// less, within reason (we only break on whitespace).
//
// In addition, this only wraps the *first line* if the given text happens to
// have newlines.
//
// For example:
//      bullet(
//          'error: something bad has happened and you need to deal:\n' +
//          'this is some context\n' +
//          'and here is some more long context that is clearer if it does not wrap',
//          30)
// would result in:
//        - error: something bad has
//          happened and you need to
//          deal:
//              this is some context
//              and here is some more long context that is clearer if it does not wrap
function bullet(text, len) {
    assert.string(text, 'text');
    assert.finite(len, 'len');

    let parts = strsplit(text, '\n', 2);
    let lines = wrappedLinesFromText(parts[0], len - 2);

    for (let i = 0; i < lines.length; i++) {
        if (i === 0) {
            lines[i] = '- ' + lines[i];
        } else {
            lines[i] = '  ' + lines[i];
        }
    }
    if (parts.length > 1) {
        for (let line of strsplit(parts[1], '\n')) {
            lines.push('  ' + line);
        }
    }

    return lines.join('\n');
}

// ---- exports

module.exports = {
    bullet: bullet,
    objCopy: objCopy,
    promptConfirm: promptConfirm,
    promptYesNo: promptYesNo
};
