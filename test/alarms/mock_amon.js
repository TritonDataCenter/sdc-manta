/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * mock_amon.js: implements a mock Amon server
 */

var assertplus = require('assert-plus');
var http = require('http');
var querystring = require('querystring');
var restifyClients = require('restify-clients');
var sdc = require('sdc-clients');
var url = require('url');
var VError = require('verror');

var account = 'mock-account-uuid';
var mockAmonPortBase = 20175;

/* Exported interface */
exports.createMockAmon = createMockAmon;
exports.account = account;

function createMockAmon(log, callback)
{
	var port, mock;

	assertplus.object(log, 'log');
	assertplus.func(callback, 'callback');

	port = mockAmonPortBase++;

	mock = { 'config': null };
	mock.log = log;
	mock.url = 'http://127.0.0.1:' + port;
	mock.client = new sdc.Amon({
	    'log': log,
	    'url': mock.url,
	    'agent': false
	});

	mock.clientRaw = restifyClients.createJsonClient({
	    'log': log,
	    'url': mock.url,
	    'agent': false
	});

	mock.server = http.createServer(
	    function handleRequest(request, response) {
		mockAmonHandleRequest(mock.log, mock.config, request, response);
	    });

	mock.server.listen(port, '127.0.0.1', function () {
		callback(mock);
	});
}

/*
 * HTTP request handler that implements our mock Amon server.  This only
 * supports the few requests that we need to implement, and it serves data based
 * on the contents of the "config" parameter, which comes from the "mock" object
 * that we gave back to the consumer.  In other words, the consumer controls
 * exactly what this server serves, and it can change it over time.  Supported
 * URLs are:
 *
 *     GET /agentprobes?agent=AGENT
 *
 *          The contents of the response are the JSON-encoded object at
 *          config.agentprobes[AGENT] (where AGENT comes from the querystring).
 *          If this value is the special string 'error', then a 500 error is
 *          returned instead.
 *
 *     GET /pub/<account>/alarms?state=STATE
 *
 *          The contents of the response are the JSON-encoded object at
 *          config.alarms[state].  If this value is the special string 'error',
 *          then a 500 error is returned instead.
 *
 *     GET /pub/<account>/alarms/ALARM_ID.
 *
 *          The contents of the response are the JSON-encoded object at
 *          config.alarms.by_id[ALARM_ID].  If this value is the special string
 *          'error', then a 500 error is returned instead.  If this value is
 *          missing, a 404 is returned.
 *
 *     POST /pub/<account>/alarms/ALARM_ID?action=ACTION
 *
 *          Uses the data in config.alarms.by_id[ALARM_ID] to determine whether
 *          the request should fail with a 500 or 404, just like the similar GET
 *          on the same path.  Successful requests complete with a 204 and
 *          record that they happened in "config.alarms_$ACTION".
 *
 *     GET /pub/<account>/probegroups
 *
 *          The contents of the response are the JSON-encoded object at
 *          config.groups.  If this value is the special string 'error', then a
 *          500 error is returned instead.
 *
 * Receiving any unsupported request or a request with bad arguments results in
 * an assertion failure.
 */
function mockAmonHandleRequest(log, config, request, response)
{
	var parsedurl, params, urlparts, value, code;

	assertplus.object(config, 'config');

	log.debug({
	    'method': request.method,
	    'url': request.url
	}, 'mock amon: handling request');

	code = 200;
	parsedurl = url.parse(request.url);
	urlparts = parsedurl.pathname.split('/');
	if (request.method == 'GET' && urlparts.length == 4 &&
	    urlparts[0] === '' && urlparts[1] == 'pub' &&
	    urlparts[2] == account && urlparts[3] == 'probegroups') {
		value = config.groups;
	} else if (request.method == 'GET' &&
	    urlparts.length == 2 && urlparts[0] === '' &&
	    urlparts[1] == 'agentprobes') {
		assertplus.object(config.agentprobes);
		params = querystring.parse(parsedurl.query);
		assertplus.string(params.agent,
		    'missing expected amon request parameter: agent');
		value = config.agentprobes[params.agent];
		if (value === undefined) {
			value = [];
		}
	} else if (request.method == 'GET' &&
	    urlparts.length == 4 && urlparts[0] === '' &&
	    urlparts[1] == 'pub' && urlparts[2] == account &&
	    urlparts[3] == 'alarms') {
		assertplus.object(config.alarms);
		params = querystring.parse(parsedurl.query);
		assertplus.string(params.state,
		    'missing expected amon request parameter: state');
		assertplus.ok(config.alarms[params.state],
		    'requested alarms for unhandled state: ' + params.state);
		value = config.alarms[params.state];
	} else if ((request.method == 'GET' || request.method == 'POST') &&
	    urlparts.length == 5 && urlparts[0] === '' &&
	    urlparts[1] == 'pub' && urlparts[2] == account &&
	    urlparts[3] == 'alarms') {
		assertplus.object(config.alarms);
		assertplus.object(config.alarms.by_id);

		if (!config.alarms.by_id[urlparts[4]]) {
			code = 404;
		} else if (request.method == 'GET' ||
		    config.alarms.by_id[urlparts[4]] == 'error') {
			value = config.alarms.by_id[urlparts[4]];
		} else {
			code = 204;
			assertplus.equal(request.method, 'POST');
			params = querystring.parse(parsedurl.query);
			assertplus.string(params.action);
			config['alarms_' + params.action].push(urlparts[4]);
		}
	} else {
		throw (new VError('unimplemented URL: %s %s',
		    request.method, request.url));
	}

	if (code == 404) {
		response.writeHead(404, {
		    'content-type': 'application/json'
		});
		response.end(JSON.stringify({
		    'code': 'NotFoundError',
		    'message': 'alarm not found'
		}));
	} else if (code == 204) {
		response.writeHead(204);
		response.end();
	} else if (value == 'error') {
		response.writeHead(500, {
		    'content-type': 'application/json'
		});
		response.end(JSON.stringify({
		    'code': 'InjectedError',
		    'message': 'injected error'
		}));
	} else {
		response.writeHead(code);
		response.end(JSON.stringify(value));
	}
}
