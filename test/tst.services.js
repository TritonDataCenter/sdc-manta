/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * tst.services.js: validates that the metadata we keep about various services
 * is complete and self-consistent.
 */

var assertplus = require('assert-plus');
var services = require('../lib/services');

/*
 * Make sure that if someone adds a new service, they've updated this test.  If
 * you're here because this test failed because you added a new service, be sure
 * to check that other consumers of service information (e.g., users of
 * mSvcNames) have been updated to reflect your new service.
 */
var knownServices = [
    'nameservice',
    'postgres',
    'moray',
    'electric-moray',
    'storage',
    'authcache',
    'webapi',
    'loadbalancer',
    'jobsupervisor',
    'jobpuller',
    'medusa',
    'ops',
    'madtom',
    'marlin-dashboard',
    'marlin',
    'propeller'
];

function main()
{
	var sharded;

	/*
	 * Check that the list of known services exactly matches the list that's
	 * configured inside lib/services.js.
	 */
	assertplus.deepEqual(knownServices.slice(0).sort(),
	    Object.keys(services.mSvcConfigsPrivate).sort());

	/*
	 * Test serviceNameIsValid().
	 */
	assertplus.deepEqual(knownServices,
	    knownServices.filter(services.serviceNameIsValid));
	assertplus.deepEqual([ 'moray' ],
	    [ 'milhouse', 'moray', 'mop' ].filter(services.serviceNameIsValid));

	/*
	 * Test serviceIsSharded().
	 */
	sharded = knownServices.filter(services.serviceIsSharded).sort();
	assertplus.deepEqual([ 'moray', 'postgres' ], sharded);

	/*
	 * Test serviceSupportsOneach().
	 */
	assertplus.deepEqual([ 'marlin' ], knownServices.filter(
	    function (svcname) {
		return (!services.serviceSupportsOneach(svcname));
	    }));

	/*
	 * Test serviceConfigProperties().
	 */
	knownServices.forEach(function (svcname) {
		if (services.serviceIsSharded(svcname)) {
			assertplus.deepEqual([ 'SH', 'IMAGE' ],
			    services.serviceConfigProperties(svcname));
		} else {
			assertplus.deepEqual([ 'IMAGE' ],
			    services.serviceConfigProperties(svcname));
		}
	});

	/*
	 * Test serviceSupportsProbes().
	 */
	assertplus.deepEqual([ 'marlin', 'propeller' ], knownServices.filter(
	    function (svcname) {
		return (!services.serviceSupportsProbes(svcname));
	    }));
	assertplus.deepEqual(services.mSvcNamesProbes,
	    services.mSvcNames.filter(services.serviceSupportsProbes));

	console.error('%s tests passed', __filename);
}

main();
