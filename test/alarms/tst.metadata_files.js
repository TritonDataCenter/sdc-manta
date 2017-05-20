/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.metadata_files.j: tests file-related interfaces in metadata subsystem.
 * This includes the functions for loading from a directory tree and the check
 * tool.
 */

var assertplus = require('assert-plus');
var extsprintf = require('extsprintf');
var forkexec = require('forkexec');
var fs = require('fs');
var path = require('path');
var vasync = require('vasync');
var VError = require('verror');

var sprintf = extsprintf.sprintf;

var alarm_metadata = require('../../lib/alarms/metadata');

var testCases = [];
var sampleEvent = 'upset.manta.test_event';
var sampleLegacyName = 'my sample probe';
var sampleScope = { 'service': 'madtom' };
var sampleChecks = [ { 'type': 'cmd', 'config': { 'test': 'prop' } } ];
var sampleKa = {
    'title': 'sample title',
    'description': 'sample description',
    'severity': 'sample severity',
    'response': 'sample response',
    'impact': 'sample impact',
    'action': 'sample action'
};
var sampleTemplate = {
    'event': sampleEvent,
    'legacyName': sampleLegacyName,
    'scope': sampleScope,
    'checks': sampleChecks,
    'ka': sampleKa
};
var done = false;
var tounlink = [];

/*
 * We create our test directory in the current directory.  Under catest, this is
 * always an appropriate working directory.  Outside of that, the user may have
 * to clean it up if we fail.
 */
var testDirectory = path.join('.', 'testdir-' + path.basename(__filename));

function main()
{
	process.on('exit', function (code) {
		if (code === 0 && !done) {
			throw (new Error('exited prematurely!'));
		}
	});

	vasync.waterfall([
	    function setupTestDirectory(callback) {
		fs.mkdir(testDirectory, function (err) {
			if (err && err.code == 'EEXIST') {
				console.error('using existing directory %s',
				    testDirectory);
				err = null;
			} else {
				console.error('creating %s', testDirectory);
			}

			callback(err);
		});
	    },

	    function setupTestFile1(callback) {
		var file, contents;
		file = path.join(testDirectory, 'file1.yaml');
		contents = JSON.stringify([ {
		    'event': sampleEvent + '.1',
		    'scope': sampleScope,
		    'checks': sampleChecks,
		    'ka': sampleKa
		}, {
		    'event': sampleEvent + '.3',
		    'scope': sampleScope,
		    'checks': sampleChecks,
		    'ka': sampleKa
		} ]);

		tounlink.push(file);
		writeFile(file, contents, callback);
	    },

	    function setupTestFile2(callback) {
		var file, contents;
		file = path.join(testDirectory, 'file2.yaml');
		contents = JSON.stringify([ {
		    'event': sampleEvent + '.2',
		    'scope': sampleScope,
		    'checks': sampleChecks,
		    'ka': sampleKa
		} ]);

		tounlink.push(file);
		writeFile(file, contents, callback);
	    },

	    function setupTestFileSkip(callback) {
		var file;
		file = path.join(testDirectory, 'file3');
		tounlink.push(file);
		writeFile(file, '{', callback);
	    },

	    function loadDirectory(callback) {
		console.error('loading data from %s', testDirectory);
		alarm_metadata.loadMetadata({
		    'directory': testDirectory
		}, callback);
	    },

	    function verifyMetadata(metadata, callback) {
		var pts;

		pts = [];
		metadata.eachTemplate(function (pt) {
			pts.push({
			    'event': pt.pt_event,
			    'origin': pt.pt_origin_label
			});
		});
		pts.sort(function (p1, p2) {
			return (p1.event.localeCompare(p2.event));
		});

		/*
		 * Importantly, we have all three probes, and it doesn't matter
		 * where each one came from.  We also skipped the file that
		 * didn't end in ".yaml".
		 */
		assertplus.deepEqual(pts, [ {
		    'event': sampleEvent + '.1',
		    'origin': sprintf('%s/file1.yaml: probe 1', testDirectory)
		}, {
		    'event': sampleEvent + '.2',
		    'origin': sprintf('%s/file2.yaml: probe 1', testDirectory)
		}, {
		    'event': sampleEvent + '.3',
		    'origin': sprintf('%s/file1.yaml: probe 2', testDirectory)
		} ]);

		callback();
	    },

	    function loadDirectoryFail(callback) {
		console.error('loading data from non-existent directory');
		alarm_metadata.loadMetadata({
		    'directory': 'junkDirectory'
		}, function (err, metadata) {
			assertplus.ok(err !== null);
			assertplus.ok(err instanceof Error);
			assertplus.ok(!metadata);
			/* JSSTYLED */
			assertplus.ok(/readdir "junkDirectory".*ENOENT/.test(
			    err.message));
			callback();
		});
	    },

	    function checkFileOkay(callback) {
		var file = path.join(testDirectory, 'file1.yaml');
		checkFile(file, function (err, info) {
			assertplus.ok(!err);
			assertplus.ok(info.status === 0);
			callback();
		});
	    },

	    function checkFileNonexistent(callback) {
		var file = path.join(testDirectory, 'ENOENT.yaml');
		checkFile(file, function (err, info) {
			assertplus.ok(err !== null);
			assertplus.ok(err instanceof Error);
			assertplus.equal(info.status, 1);
			/* JSSTYLED */
			assertplus.ok(/read ".*ENOENT.yaml": ENOENT/.test(
			    info.stderr));
			callback();
		});
	    },

	    function checkFileInvalid(callback) {
		var file = path.join(testDirectory, 'file3');
		checkFile(file, function (err, info) {
			assertplus.ok(err !== null);
			assertplus.ok(err instanceof Error);
			assertplus.equal(info.status, 1);
			assertplus.ok(
			    /* JSSTYLED */
			    /parse ".*file3": unexpected end of the stream/.
			    test(info.stderr));
			callback();
		});
	    },

	    function cleanupFiles(callback) {
		console.error('removing files: %s', tounlink.join(', '));
		vasync.forEachPipeline({
		    'inputs': tounlink,
		    'func': fs.unlink
		}, function (err) {
			callback(err);
		});
	    },

	    function cleanupDirectory(callback) {
		console.error('removing directory: %s', testDirectory);
		fs.rmdir(testDirectory, callback);
	    }
	], function (err) {
		if (err) {
			throw (err);
		}

		done = true;
		console.error('%s okay', __filename);
	});
}

/*
 * Wrapper around fs.writeFile() that provides a more useful Error on failure.
 */
function writeFile(filename, contents, callback)
{
	console.error('writing %s', filename);
	fs.writeFile(filename, contents, function (err) {
		if (err) {
			err = new VError('write "%s"', filename);
		}

		callback(err);
	});
}

/*
 * Runs the checker tool on the specified file.
 */
function checkFile(filename, callback)
{
	var argv = [
	    path.join(__dirname, '..', '..', 'tools', 'probecfgchk.js'),
	    filename
	];

	console.error('checking %s', filename);
	forkexec.forkExecWait({
	    'argv': argv
	}, function (err, info) {
		callback(err, info);
	});
}

main();
