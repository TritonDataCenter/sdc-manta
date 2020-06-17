/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * Note: This is adapted from <sdcadm.git:lib/cli/ui.js>.
 *
 * A class to capture all/most output on the CLI. Typically the CLI object
 * will have an instance of this at `this.ui`.
 *
 * Usage in code for a CLI tool using this:
 *
 * - get the `<cli>.ui` object
 * - `ui.info(...)` for printf-style message output to stdout
 * - `ui.error(...)` for printf-style error message output to *stderr*. If on
 *   a TTY, this is colored red. Otherwise it is the same as `ui.info`.
 * - To use a progress bar:
 *      - call `ui.barStart({name: 'NAME', ...})`
 *      - call `ui.barAdvance(N)` to advance progress
 *      - call `ui.barEnd()` when complete.
 *   These methods know to avoid using a progress bar if output is not to a
 *   TTY. `ui.info` and `ui.error` know to use `<bar>.log` when a progress bar
 *   is active.
 */

// jsl:ignore
'use strict';
// jsl:end

var format = require('util').format;

var assert = require('assert-plus');
var ProgressBar = require('progbar').ProgressBar;
var VError = require('verror');

var clicommon = require('./clicommon');

// ---- globals

var noColorEnvVarName = 'MANTA_NO_COLOR';

// ---- internal support stuff

// http://en.wikipedia.org/wiki/ANSI_escape_code#graphics
// Suggested colors (some are unreadable in common cases):
// - Good: cyan, yellow (limited use), bold, green, magenta, red
// - Bad: grey (same color as background on Solarized Dark theme from
//   <https://github.com/altercation/solarized>, see issue #160)
var colors = {
    bold: [1, 22],
    italic: [3, 23],
    underline: [4, 24],
    inverse: [7, 27],
    white: [37, 39],
    grey: [90, 39],
    black: [30, 39],
    blue: [34, 39],
    cyan: [36, 39],
    green: [32, 39],
    magenta: [35, 39],
    red: [31, 39],
    yellow: [33, 39]
};

function stylizeWithColor(str, color) {
    if (!str) {
        return '';
    }
    var codes = colors[color];
    if (codes) {
        return '\x1b[' + codes[0] + 'm' + str + '\x1b[' + codes[1] + 'm';
    } else {
        return str;
    }
}

function stylizeWithoutColor(str, _color) {
    return str;
}

// ---- UI

function UI(opts) {
    assert.object(opts.log, 'opts.log');
    assert.optionalBool(opts.color, 'opts.color');
    assert.optionalBool(opts.quiet, 'opts.quiet');
    assert.optionalBool(opts.noProgress, 'opts.noProgress');

    this.log = opts.log.child({ui: true}, true);

    // We support ANSI escape code coloring (currently just used for `ui.error`)
    // if writing to a TTY. Use `$noColorEnvVarName=1` envvar to disable.
    var color = opts.color;
    if (color === null || color === undefined) {
        if (
            process.env[noColorEnvVarName] &&
            process.env[noColorEnvVarName].length > 0
        ) {
            color = false;
        } else {
            color = process.stdout.isTTY;
        }
    }
    this._stylize = color ? stylizeWithColor : stylizeWithoutColor;

    this.quiet = Boolean(opts.quiet);
    this.noProgress = Boolean(opts.noProgress);
}

UI.prototype.info = function info() {
    var msgArgs = Array.prototype.slice.call(arguments);
    var msg = format.apply(null, msgArgs);
    this.log.debug(msg);
    if (this._bar) {
        this._bar.log(msg);
    } else {
        console.log(msg);
    }
};

// Write the msg (from the given printf-style args) as a Markdown bullet
// with `- ` bullet marker and wrapping the first line at 80 columns.
UI.prototype.infoBullet = function infoBullet() {
    var msgArgs = Array.prototype.slice.call(arguments);
    var msg = clicommon.bullet(format.apply(null, msgArgs), 80);
    this.log.debug(msg);
    if (this._bar) {
        this._bar.log(msg);
    } else {
        console.log(msg);
    }
};

UI.prototype.error = function error() {
    var msgArgs = Array.prototype.slice.call(arguments);
    var msg = format.apply(null, msgArgs);
    this.log.debug(msg);
    var styled = this._stylize(msg, 'red');
    if (this._bar) {
        this._bar.log(styled);
    } else {
        console.error(styled);
    }
};

// -- progress bars

// Start a progress bar.
//
// This will be a no-op for cases where a progress bar is inappropriate
// (e.g. if stderr is not a TTY).
UI.prototype.barStart = function barStart(opts) {
    assert.string(opts.name, 'opts.name');
    assert.optionalFinite(opts.size, 'opts.size');
    assert.optionalBool(opts.bytes, 'opts.bytes');
    assert.optionalFinite(opts.drawDelay, 'opts.drawDelay');

    if (this.quiet || this.noProgress) {
        // shhh
        // jsl:pass
    } else if (this._bar) {
        throw new VError(
            'another progress bar (%s) is currently active',
            this._bar.filename
        );
    } else if (process.stderr.isTTY) {
        var barOpts = {
            filename: opts.name,
            // ProgressBar began life assuming it was progress for a file
            // download. Hence `*file*name`. To avoid it appending size suffixes
            // like "KB" and "MB" by default, we require an explicit
            // `bytes: true`.
            bytes: Boolean(opts.bytes)
        };
        if (opts.size) {
            barOpts.size = opts.size;
        } else {
            barOpts.nosize = true;
        }
        var bar = (this._bar = new ProgressBar(barOpts));

        // We hack into `bar.pb_done` to trick it to *not* draw for a startup
        // delay. This allows the UI to avoid showing the progress bar unless
        // it is a longer process.
        //
        // TODO: It would be much better to add native functionality for this
        // to node-progbar.
        if (opts.drawDelay) {
            this._bar.pb_done = true;
            this._barStartTimeout = setTimeout(function allowBarDraw() {
                bar.draw();
                bar.pb_done = false;
            }, opts.drawDelay);
        } else {
            bar.draw();
        }
    }

    return this._bar;
};

UI.prototype.barStream = function barStream() {
    if (this._bar) {
        return this._bar.stream();
    } else {
        return null;
    }
};

UI.prototype.barEnd = function barEnd() {
    if (this._bar) {
        this._bar.end();
        delete this._bar;
        if (this._barStartTimeout) {
            clearTimeout(this._barStartTimeout);
        }
    }
};

UI.prototype.barAdvance = function barAdvance(n) {
    if (this._bar) {
        this._bar.advance(n);
    }
};

// --- Mock UI

// Create a mock `UI` instance. The optional `opts.write` allows, for example,
// a test to do:
//
//      ui = new MockUI({write: tap.comment});
//
function MockUI(opts) {
    assert.optionalFunc(opts.write, 'opts.write');
    this._write = opts.write || console.log;
}

MockUI.prototype.info = function mockInfo() {
    var msgArgs = Array.prototype.slice.call(arguments);
    var msg = format.apply(null, msgArgs);
    this._write(msg);
};

MockUI.prototype.error = function mockError() {
    var msgArgs = Array.prototype.slice.call(arguments);
    var msg = format.apply(null, msgArgs);
    this._write(msg);
};

MockUI.prototype.barStart = function mockBarStart(_opts) {};
MockUI.prototype.barEnd = function mockBarEnd() {};
MockUI.prototype.barAdvance = function mockBarAdvance(_n) {};

// --- exports

module.exports = {
    UI: UI,
    MockUI: MockUI
};
