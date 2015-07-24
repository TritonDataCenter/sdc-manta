/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * CollectorStream: transform that collects all input and makes it available as
 * a single string
 */

var readable = require('readable-stream');
var util = require('util');

module.exports = CollectorStream;

function CollectorStream(options)
{
	readable.Transform.call(this, options);
	this.data = '';
}

util.inherits(CollectorStream, readable.Transform);

CollectorStream.prototype._transform = function (chunk, encoding, done)
{
	this.data += chunk;
	done();
};

CollectorStream.prototype._flush = function (callback)
{
	callback();
};
