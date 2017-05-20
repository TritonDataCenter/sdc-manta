/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/instance_info.js: represents abstract information about a specific
 * instance (a VMAPI VM).
 */

var assertplus = require('assert-plus');

/* Exported interface */
exports.InstanceInfo = InstanceInfo;

/*
 * InstanceInfo represents information about a specific instance.  This should
 * eventually include SAPI and VMAPI information.  This is the abstract object
 * we should pass around between different subsystems, rather than raw VMAPI and
 * SAPI objects.  For now, this is only used for the alarms subsystem.
 */
function InstanceInfo(args)
{
	assertplus.object(args, 'args');
	assertplus.string(args.uuid, 'args.uuid');
	assertplus.string(args.svcname, 'args.svcname');
	assertplus.object(args.metadata, 'args.metadata');
	assertplus.bool(args.local, 'args.local');
	assertplus.optionalString(args.server_uuid, 'args.server_uuid');

	if (args.local) {
		assertplus.string(args.server_uuid, 'args.server_uuid');
	} else {
		/*
		 * There's no way for us to authoritatively know the server_uuid
		 * for remote instances.  There's a server uuid in the SAPI
		 * information, but it may be out of date if the VM has been
		 * migrated or the server's chassis has been swapped or the
		 * like.  The only authoritative place for this information is
		 * in the VMAPI data, which we don't have for remote instances.
		 */
		assertplus.strictEqual(args.server_uuid, null);
	}

	/* VM uuid */
	this.inst_uuid = args.uuid;

	/* SAPI service name to which this instance belongs */
	this.inst_svcname = args.svcname;

	/* SAPI metadata for this instance */
	this.inst_metadata = args.metadata;

	/* Boolean indicating whether this instance is in this datacenter. */
	this.inst_local = args.local;

	/* Server uuid where this VM lives, or "null" for non-local instances */
	this.inst_server_uuid = args.server_uuid;
}
