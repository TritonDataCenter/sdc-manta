/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * Validate the networking parameters.
 */
var mod_fs = require('fs');

function fatal(message)
{
	console.error('validate.js: validation failure ' + message);
	process.exit(1);
}

function main()
{
	var spec, az, i;

	if (process.argv.length !== 3) {
		console.error('validate.js: <file>');
		process.exit(1);
	}
	spec = JSON.parse(mod_fs.readFileSync(process.argv[2]));

	if (!('mac_mappings' in spec))
		fatal('missing mac_mappings');

	if (!('marlin_nodes' in spec))
		fatal('missing marlin_nodes');

	if (!Array.isArray(spec['marlin_nodes']))
		fatal('marlin_nodes should be an array');

	if (spec['marlin_nodes'].length === 0)
		fatal('no marlin CNs listed');

	for (i = 0; i < spec['marlin_nodes'].length; i++) {
		if (!(spec['marlin_nodes'][i] in spec['mac_mappings']))
			fatal('missing node from mac_mappings: ' +
			    spec['marlin_nodes'][i]);
		if (!(spec['marlin']['nic_tag'] in
		    spec['mac_mappings'][spec['marlin_nodes'][i]])) {
			fatal('missing tag ' + spec['marlin']['nic_tag'] +
			    ' in mac_mappings.' + spec['marlin_nodes'][i]);
		}
	}

	if (!('manta_nodes' in spec))
		fatal('missing manta_nodes');

	if (!Array.isArray(spec['manta_nodes']))
		fatal('manta_nodes should be an array');

	if (spec['manta_nodes'].length === 0)
		fatal('no indexing CNs listed');

	for (i = 0; i < spec['manta_nodes'].length; i++) {
		if (!(spec['manta_nodes'][i] in spec['mac_mappings']))
			fatal('missing node from mac_mappings: ' +
			    spec['manta_nodes'][i]);
		if (!(spec['manta']['nic_tag'] in
		    spec['mac_mappings'][spec['manta_nodes'][i]])) {
			fatal('missing tag ' + spec['manta']['nic_tag'] +
			    ' in mac_mappings.' + spec['manta_nodes'][i]);
		}
	}

	if (!('azs' in spec))
		fatal('missing availability zone list');

	if (!Array.isArray(spec['azs']))
		fatal('azs should be an array');

	if (spec['azs'].length === 0)
		fatal('no azs listed');

	if (!('admin' in spec))
		fatal('missing admin network information');

	if (!('manta' in spec))
		fatal('missing manta network information');

	if (!('marlin' in spec))
		fatal('missing marlin network information');

	if (!('nic_tag' in spec['admin']))
		fatal('admin section missing nic tag name');

	if (!('network' in spec['admin']))
		fatal('admin section missing network name');

	if (!('nic_tag' in spec['manta']))
		fatal('manta section missing nic tag name');

	if (!('network' in spec['manta']))
		fatal('manta section missing network name');

	if (!('nic_tag' in spec['marlin']))
		fatal('marlin section missing nic tag name');

	if (!('network' in spec['marlin']))
		fatal('marlin section missing network name');

	for (i = 0; i < spec['azs'].lenght; i++) {
		az = spec['azs'][i];
		if (!(az in spec['admin']))
			fatal('missing information for az ' + az +
			    'in the admin block');

		if (!(az in spec['manta']))
			fatal('missing information for az ' + az +
			    'in the manta block');

		if (!(az in spec['marlin']))
			fatal('missing information for az ' + az +
			    'in the marlin block');
	}
}

main();
