/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * tst.stripe.js: tests the stripe() utility function
 */

var assert = require('assert');
var common = require('../lib/common');
var stripe = common.stripe;

/* invalid input */
assert.throws(function () { stripe(); }, /lists.*array.*required/);
assert.throws(function () { stripe(true); }, /lists.*array.*required/);
assert.throws(function () { stripe({}); }, /lists.*array.*required/);
assert.throws(function () { stripe([ true ]); }, /lists.*array.*required/);

/* degenerate cases */
assert.deepEqual(stripe([]), []);
assert.deepEqual(stripe([ [] ]), []);
assert.deepEqual(stripe([ [ 0, 4, 8, 12 ] ]), [ 0, 4, 8, 12 ]);

/* simple cases */
assert.deepEqual(stripe([
    [ 0, 4, 8, 12 ],
    [ 2, 6, 10, 14 ]
]), [ 0, 2, 4, 6, 8, 10, 12, 14 ]);
assert.deepEqual(stripe([
    [ 2, 6, 10, 14 ],
    [ 0, 4, 8, 12 ]
]), [ 2, 0, 6, 4, 10, 8, 14, 12 ]);
assert.deepEqual(stripe([
    [ 0, 4,  8, 12 ],
    [ 1, 5,  9, 13 ],
    [ 2, 6, 10, 14 ],
    [ 3, 7, 11, 15 ]
]), [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 ]);

/* first array smaller than the others */
assert.deepEqual(stripe([
    [ 0, 4 ],
    [ 1, 5,  9, 13 ],
    [ 2, 6, 10, 14 ],
    [ 3, 7, 11, 15 ]
]), [ 0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 13, 14, 15 ]);
/* last array smaller than the others */
assert.deepEqual(stripe([
    [ 0, 4,  8, 12 ],
    [ 1, 5,  9, 13 ],
    [ 2, 6, 10, 14 ],
    [ 3, 7 ]
]), [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14 ]);
/* different length arrays, some empty to start with */
assert.deepEqual(stripe([
    [ ],
    [ 0, 1, 2, 3 ],
    [ 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 ],
    [ 14, 15, 16 ],
    [ ]
]), [ 0, 4, 14, 1, 5, 15, 2, 6, 16, 3, 7, 8, 9, 10, 11, 12, 13 ]);

/*
 * stripe() doesn't care what's in these arrays, but it's worth verifying that
 * it works with more complex objects.
 */
assert.deepEqual(stripe([
    [ [ 1, 2, 3 ] ],
    [ [ 6, 5, 4 ] ],
    [ null, null ],
    [ { 'foofaraw': 'arglebargle' } ]
]), [ [ 1, 2, 3 ], [ 6, 5, 4 ], null, { 'foofaraw': 'arglebargle' }, null ]);

console.log('TEST PASSED');
