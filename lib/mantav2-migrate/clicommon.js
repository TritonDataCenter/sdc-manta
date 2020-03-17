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

// ---- exports

module.exports = {
    objCopy: objCopy,
    promptYesNo: promptYesNo
};
