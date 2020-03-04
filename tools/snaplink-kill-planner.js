#!/usr/node/bin/node
/*
 * Copyright 2020 Joyent, Inc.
 *
 * Note: This assumes the number of SnapLinks is small enough that we'll not run
 * out of memory building a mapping. If there are many millions, you might need
 * to be careful.
 *
 */

var fs = require('fs');
var path = require('path');
var readline = require('readline');
var util = require('util');

var uuid = require('/usr/node/0.10/node_modules/uuid.node');

var lineReader = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});


var metadataCommands = [];
var numLines = 0;
var storageCommands = {};
var zeroSizedObjects = 0;

var shardId = process.argv[2];

var BASH_HEADER = [
    '#!/bin/bash',
    '',
    'set -o errexit',
    'set -o pipefail',
    'xtrace_log="$(date -u +%Y%m%dT%H%M%S).xtrace.log"',
    'echo "Writing xtrace output to: ${xtrace_log}" >&2',
    'exec 4>>$xtrace_log',
    'BASH_XTRACEFD=4',
    'set -o xtrace',
    ''
].join('\n') + '\n';

if (typeof (shardId) !== 'string') {
    console.error('Usage: snaplink-kill-planner.js <shardId>');
    process.exit(1);
}

lineReader.on('line', function _onLine(rawLine) {
    var idx;
    var newUuid = uuid.create();
    var obj;
    var storageId;

    numLines++;
    obj = JSON.parse(rawLine); // Will blow up on error.

    // First: prepare the "storage" zone update if needed.

    if (obj.size === 0) {
        zeroSizedObjects++;
    } else {
        for (idx = 0; idx < obj.storageIds.length; idx++) {
            storageId = obj.storageIds[idx];
            if (!storageCommands.hasOwnProperty(storageId)) {
                storageCommands[storageId] = [];
            }
            //
            // Add an `ln <old> <new>` instruction for each `storage` zone that
            // has this object.
            //
            storageCommands[storageId].push([
                'ln /manta/', obj.creatorId, '/', obj.objectId,
                ' /manta/', obj.creatorId, '/', newUuid
            ].join(''));
        }
    }

    // Second: prepare the script for the update to Moray.
    //
    // Basically we just need to rewrite each object, so we're just going to do
    // a getobject, transform and then putobject.
    //
    if (obj.key.indexOf('"') !== -1 || obj.key.indexOf('\\') !== -1) {
        throw new Error('key must not have " or \\ characters. Got: ' + obj.key);
    }

    //
    // E.g. putobject -d $(getobject manta "/771e0f61-f938-4678-87d1-33381702ed6f/stor/hello1.txt" \
    //          | json -o json-0 -e "this.value.objectId='6e09bf84-ce29-602d-d58c-92765e72124a'" value) \
    //          manta "/771e0f61-f938-4678-87d1-33381702ed6f/stor/hello1.txt"
    //
    metadataCommands.push([
        'putobject -d $(getobject manta "',
        obj.key,
        '" | json -o json-0 -e "this.value.objectId=\'',
        newUuid,
        '\'" value) manta "',
        obj.key,
       '"'
    ].join(''));

}).on('close', function() {
    var dir = shardId + '.' + new Date().toISOString().replace(/[\-\:\.]/g, '');
    var filename;
    var idx;
    var storageId;
    var storageIds;

    console.error('Writing files to: ./' + dir + '/');

    fs.mkdir(dir, function _onMkdir(err) {
        if (err) {
            // ¯\_(ツ)_/¯
            throw (err);
        }

        // Write out a file with the moray updates required
        filename = path.join(dir, shardId + '.sh');
        console.error('Writing ./' + filename);
        fs.writeFileSync(filename,
            BASH_HEADER +
            metadataCommands.join('\n') + '\n',
            {encoding: 'utf8'});

        // Write out a shell script for each storage zone we need to update
        storageIds = Object.keys(storageCommands);
        for (idx = 0; idx < storageIds.length; idx++) {
            storageId = storageIds[idx];
            filename = path.join(dir, storageId + '.sh');
            console.error('Writing ./' + filename);
            fs.writeFileSync(filename,
                BASH_HEADER +
                storageCommands[storageId].join('\n') + '\n',
                {encoding: 'utf8'});
        }

        // Summary!
        console.error(util.format('Lines: %d, mdata Updates: %d',
            numLines, metadataCommands.length));
    });
});
