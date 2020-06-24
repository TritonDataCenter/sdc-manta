/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

function do_completion(subcmd, opts, _args, cb) {
    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    }

    if (opts.raw) {
        console.log(this.bashCompletionSpec());
    } else {
        console.log(this.bashCompletion());
    }
    cb();
}

do_completion.options = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Show this help.'
    },
    {
        names: ['raw'],
        type: 'bool',
        hidden: true,
        help:
            'Only output the Bash completion "spec". ' +
            'This is only useful for debugging.'
    }
];
do_completion.help = [
    'Emit bash completion.',
    '',
    'Installation (TritonDC):',
    '    {{name}} completion > /etc/bash/bash_completion.d/{{name}} &&',
    '        source /etc/bash/bash_completion.d/{{name}}',
    '',
    'Unfortunately this is wiped on headnode reboot.',
    '',
    '{{options}}'
].join('\n');

do_completion.hidden = true;

module.exports = do_completion;
