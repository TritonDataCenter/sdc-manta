#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace

# Hard code the image uuids now cause I don't want to keep downloading and
# installing if I don't have to...
# updates-imgadm list name=manta-storage | tail -1
STOR_IMAGE_UUID=b4d54ad6-e75f-11e3-97dc-bfb25dfdd6e0
WEBAPI_IMAGE_UUID=e07e9b1c-ed07-11e3-8dd2-6b59ee0ada41
LOADBALANCER_IMAGE_UUID=eb7002c8-f265-11e3-99b1-6f9a0af6e5dc
JOBSUPERVISOR_IMAGE_UUID=aba08db2-f205-11e3-a80c-9fd97da75b15
JOBPULLER_IMAGE_UUID=842383f8-f0f1-11e3-bb68-fbfb1850dfcd
MARLIN_IMAGE_UUID=b4bdc598-8939-11e3-bea4-8341f6861379 #sdc-multiarch 13.3.1
MEDUSA_IMAGE_UUID=199380b4-f273-11e3-a9ad-2bfb2e3bc625
OPS_IMAGE_UUID=a8e0e44c-f2b8-11e3-9ada-0b8d65ec8f04

ROOT=/opt/smartdc/manta-deployment
DIR=$ROOT/skate
MD=/var/tmp/metadata.json
SIZE=coal
O=/var/tmp/skate
CURL="curl -s -H accept:application/json -H content-type:application/json"
SAPI=http://$(json -f $MD SAPI_SERVICE)
CNAPI=http://$(json -f $MD CNAPI_SERVICE)
DNS_DOMAIN=$(json -f $MD dns_domain)
DATACENTER_NAME=$(json -f $MD datacenter_name)
SERVER_UUID=$($CURL $CNAPI/servers | json -Ha uuid)
COMPUTE_ID=1.skate-cn.${DATACENTER_NAME}.${DNS_DOMAIN}

SSL_CERT='-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDtyExfwZGOp0kV\nRBtGSdX1aPJZvl/w2fR0QvER65QVKE5QukX3zSl1xP6BWnXEO8LtukHo3cbWnXao\n4aFMLH21LNdlsJCq8JCPDTmBRmqSyT3ZQEWv0PXQCRtzkfw8CdprFrEbZC77q9uz\nTdt48JJccS647+S8F+b6ANt5QQeCuU9a5/TRa8qIAB6pOKpeJpfmrNijLVrn5S+/\niYjnLiOhvDRJIFKatMXfArT5M6u1QNoB4ex0J/Ae28hwnzfdDiwCmS9eyoWzBlXI\nhOO+GYzuU7csfMhC9mkGDq1+1VJt1aV2mZHBmy5dAPh7sUEeFV7j7dzljQMuGbKL\nFnoteUvvAgMBAAECggEAfsjN8EDF28pWrYAjCtdTvga3MiLQBRwxu8v1vhheXOmg\n2Gp55CcAdEsVaONS5871oMwvWHroCobrPiEQYA1Y+cFvGEmM2Zhre9sRTly9dobW\n83/RqG1CpCo2+OheFaUrUO5IgNJqOsbn3jMlUtbzM1cmEnXXAHO+NI9Adua1mn0t\nN2H+9Jb9Y4LWTkeoRTCOps1GydPdVA96WND2XiU1XhA1I7rjgwVyKE4CQvLldDZs\nfG/Ia7F7SeIk6gy3b/E+VkW5UMhEuRcldfKI9rVogXsSSqVfaXjQdOZdoEI9e3hQ\n4q//pQczTBebN7jSPDFqXDlUa4rGLq3rTiJqKpMgAQKBgQD2ueLM/+4XbjRm5nG/\nYKBQB2C1nEmng7qGax/jRFuzVjar5CznMWAXeLz/KBjpDpX5/AlmQMvQqB3UMi8z\n0x1PKTIWIFA12gQGcBlKf2SR35VYn/S1mipUya43/xuORoZ0/Lmt5hs1Nf0XklX+\nzG6UVAqMqzV15+ayu/MZMapo4QKBgQD2uFoXPhtXPl2UeHoX6D5mLWSnE9KI5qbO\nEyhdFfXt7T0v3gjvLXwlJ+mAPfXkl3WnYvFWpN6mh9/WnbjpzpDgE4MdxI+43QZO\nT84RcOMratlrKpmU5RpshC4nnm/vxlRBoFCih/3rafuesHohlDQbG1kcYfeKCkWQ\nP5CuQEg+zwKBgQDJq52wVU4HRuR8a8b0WIYRhEZPjGVEEM+pSezxtpGCIGllzYfK\nnjheVymdIuhhr7N1d8czFqnCb3iaVz7wDJN7fj/biCQoMIFzxbNWipTpC6mmnHDI\n9C+/yG6ohNTAUfNbsqwPcfQxZWTwHIRAVlfY1G0fGF8Fdj3DQQ/vOwKA4QKBgG7W\nTxLWAe5lxCCpJyCKY03/4pH0o8aA6Dr3/FAlR4CF7dENTLI2mukOTyYfr9HsAR0b\npBQqqSPjMrn1zVasSAWCform/TItiWGxFIBdWJDuxZIRZ0Gu0vohP6Lo2Jy9WqjI\n3rDFq1sRF8souRVQwT/UP2oy0MQg7TNqexLtKgedAoGBANoI98X9dNYczY0UwYEH\ngHNz537cFTw0Vdz2zO60iIneXsPZjJQJ/Jstib3iqdvrPUBY4ruC+YEjJRxEVnnG\nFBhR9lPKOLTEDbGzwc0nzAbh4dU6MObScSQ8jUm114aA5NKJnG/+uI3VMEZ9ZvVQ\nSOtbYYfpUzC+MZ/B5f1JqfiM\n-----END PRIVATE KEY-----\n-----BEGIN CERTIFICATE-----\nMIIEADCCAuigAwIBAgIJAIUnM58xjwdMMA0GCSqGSIb3DQEBBQUAMF0xCzAJBgNV\nBAYTAlVTMQswCQYDVQQIEwJDQTEPMA0GA1UEChMGSm95ZW50MQ4wDAYDVQQLEwVt\nYW50YTEgMB4GA1UEAxMXbWFudGEuc3RhZ2luZy5qb3llbnQudXMwHhcNMTQwMzEy\nMjExOTE0WhcNMTUwMzEyMjExOTE0WjBdMQswCQYDVQQGEwJVUzELMAkGA1UECBMC\nQ0ExDzANBgNVBAoTBkpveWVudDEOMAwGA1UECxMFbWFudGExIDAeBgNVBAMTF21h\nbnRhLnN0YWdpbmcuam95ZW50LnVzMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB\nCgKCAQEA7chMX8GRjqdJFUQbRknV9WjyWb5f8Nn0dELxEeuUFShOULpF980pdcT+\ngVp1xDvC7bpB6N3G1p12qOGhTCx9tSzXZbCQqvCQjw05gUZqksk92UBFr9D10Akb\nc5H8PAnaaxaxG2Qu+6vbs03bePCSXHEuuO/kvBfm+gDbeUEHgrlPWuf00WvKiAAe\nqTiqXiaX5qzYoy1a5+Uvv4mI5y4jobw0SSBSmrTF3wK0+TOrtUDaAeHsdCfwHtvI\ncJ833Q4sApkvXsqFswZVyITjvhmM7lO3LHzIQvZpBg6tftVSbdWldpmRwZsuXQD4\ne7FBHhVe4+3c5Y0DLhmyixZ6LXlL7wIDAQABo4HCMIG/MB0GA1UdDgQWBBRYQBIH\nt+2r4DyOOt0PBcGik+ooMzCBjwYDVR0jBIGHMIGEgBRYQBIHt+2r4DyOOt0PBcGi\nk+ooM6FhpF8wXTELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMQ8wDQYDVQQKEwZK\nb3llbnQxDjAMBgNVBAsTBW1hbnRhMSAwHgYDVQQDExdtYW50YS5zdGFnaW5nLmpv\neWVudC51c4IJAIUnM58xjwdMMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQAD\nggEBAL21seqVk494Vz1K8Nhn6viUC+X0PY/5ChyBjQj7coFiAf4kfUK094dAjvDb\nHG3sxyvqyIgGJPUNeSsAFNJmhvora0rqcylFiofy8xkJzOeSKpLCyTAjcZjlNXbu\nwWd8axcb4bpE/epIhsfJXsAXO5plWk9rL4as74uAc3i91MTN0CEj9R8pPk1K79ZJ\nGLJ1ZO5xIDw2x++kIc9DiyCBRk1iWWLArSzyMpIohHwwZJPMiEJ99IXiZRDfvAEx\nqQR4fic05uB3Dbq5pFegK3jPKZpDAyFli23RUtgbqO4TZnIiepZ9ob8plikloKDO\nBP3tIwgZMinJBiEVRiEzgXQE9zY=\n-----END CERTIFICATE-----\n'

mkdir -p $O

function bootstrapConfigs() {
    mkdir -p $DIR/templates
    rm $DIR/templates/*

    ls $DIR/sapi_manifests | while read l; do
        ln -s $DIR/sapi_manifests/$l $ROOT/sapi_manifests/$l
    done

    svcadm restart config-agent
}

# --- Skate application
function skateApplication() {
    SKATE_APP=$($CURL $SAPI/applications?name=skate | json -Ha uuid)
    if [[ -n "$SKATE_APP" ]]; then
        echo "Skate application already exists."
        return
    fi

    while [[ ! -f $DIR/templates/application.json.in ]]; do
        sleep 1;
    done

    APP_JSON=$O/application.json
    ADMIN_PRIVATE_KEY=$(cat $MD | grep SDC_PRIVATE_KEY | cut -d '"' -f 4)
    ADMIN_PUBLIC_KEY=$(cat $MD | grep SDC_PUBLIC_KEY | cut -d '"' -f 4)

    # Pull the Manta app manifests for Skate
    MANTA_APP=$($CURL $SAPI/applications?name=manta | json -H 0)
    if [[ -z "$MANTA_APP" ]]; then
    # This will fail, but it'll get the application and manifests in place.
        manta-init -n -e nobody@joyent.com -s $SIZE
        MANTA_APP=$($CURL $SAPI/applications?name=manta | json -H 0)
    fi
    MANIFESTS=$(echo "$MANTA_APP" | json manifests | tr -s '"' "'" | tr -d '\n')

    json -f $DIR/templates/application.json.in -e " \
        this.metadata.SIZE = '${SIZE}'; \
        this.metadata.ADMIN_PRIVATE_KEY = '${ADMIN_PRIVATE_KEY}'; \
        this.metadata.ADMIN_PUBLIC_KEY = '${ADMIN_PUBLIC_KEY}'; \
        this.metadata.SERVER_COMPUTE_ID_MAPPING = { \
             '${SERVER_UUID}': '${COMPUTE_ID}' \
        }; \
        this.manifests = ${MANIFESTS}; \
    " >$APP_JSON

    $CURL $SAPI/applications -X POST -d@$APP_JSON
    SKATE_APP=$($CURL $SAPI/applications?name=skate | json -Ha uuid)
}

# --- Stor service
function storService() {
    STOR_SERVICE=$($CURL $SAPI/services?name=skate-storage | json -Ha uuid)
    if [[ -n "$STOR_SERVICE" ]]; then
        echo "Skate storage service already exists."
        return
    fi

    STOR_SERVICE_JSON=$O/storage-service.json
    json -f $ROOT/config/services/storage/service.json -e " \
        this.name = 'skate-storage';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${STOR_IMAGE_UUID}'; \
    " >$STOR_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$STOR_SERVICE_JSON
    STOR_SERVICE=$($CURL $SAPI/services?name=skate-storage | json -Ha uuid)
}

# --- Stor instance
function storInstances() {
    STOR_INSTANCES=$($CURL $SAPI/instances?service_uuid=$STOR_SERVICE | \
        json -Ha uuid)
    COUNT=$(echo "$STOR_INSTANCES" | wc -w)
    if [[ -n "$STOR_INSTANCES" ]] && [[ $COUNT == 2 ]]; then
        echo "Skate storage instances already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-stor-instance.json.in ]]; do
        sleep 1;
    done

    while [[ "$COUNT" != "2" ]]; do
        let COUNT=$COUNT+1
        STOR_INSTANCE_JSON=$O/storage-instance-$COUNT.json
        STORAGE_ID=${COUNT}.skate-stor.${DATACENTER_NAME}.${DNS_DOMAIN}
        json -f $DIR/templates/skate-stor-instance.json.in -e " \
            this.service_uuid = '${STOR_SERVICE}'; \
            this.params.alias = '${STORAGE_ID}'; \
            this.params.server_uuid = '${SERVER_UUID}'; \
            this.params.tags.manta_storage_id = '${STORAGE_ID}'; \
            this.metadata.MANTA_STORAGE_ID = '${STORAGE_ID}'; \
        " >$STOR_INSTANCE_JSON

        $CURL $SAPI/instances -X POST -d@$STOR_INSTANCE_JSON
    done
}

# --- Webapi service
function webapiService() {
    WEBAPI_SERVICE=$($CURL $SAPI/services?name=skate-webapi | json -Ha uuid)
    if [[ -n "$WEBAPI_SERVICE" ]]; then
        echo "Skate webapi service already exists."
        return
    fi

    WEBAPI_SERVICE_JSON=$O/webapi-service.json
    json -f $ROOT/config/services/webapi/service.json -e " \
        this.name = 'skate-webapi';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${WEBAPI_IMAGE_UUID}'; \
    " >$WEBAPI_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$WEBAPI_SERVICE_JSON
    WEBAPI_SERVICE=$($CURL $SAPI/services?name=skate-webapi | json -Ha uuid)
}

# --- Webapi instance
function webapiInstance() {
    WEBAPI_INSTANCE=$($CURL $SAPI/instances?service_uuid=$WEBAPI_SERVICE | \
        json -Ha uuid)
    if [[ -n "$WEBAPI_INSTANCE" ]]; then
        echo "Skate webapi instance already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-webapi-instance.json.in ]]; do
        sleep 1;
    done

    WEBAPI_INSTANCE_JSON=$O/webapi-instance.json
    json -f $DIR/templates/skate-webapi-instance.json.in -e " \
        this.service_uuid = '${WEBAPI_SERVICE}'; \
        this.params.server_uuid = '${SERVER_UUID}'; \
    " >$WEBAPI_INSTANCE_JSON

    $CURL $SAPI/instances -X POST -d@$WEBAPI_INSTANCE_JSON
    WEBAPI_INSTANCE=$($CURL $SAPI/instances?service_uuid=$WEBAPI_SERVICE | \
        json -Ha uuid)
}

# --- Loadbalancer service
function loadbalancerService() {
    LOADBALANCER_SERVICE=$($CURL $SAPI/services?name=skate-loadbalancer | json -Ha uuid)
    if [[ -n "$LOADBALANCER_SERVICE" ]]; then
        echo "Skate loadbalancer service already exists."
        return
    fi

    LOADBALANCER_SERVICE_JSON=$O/loadbalancer-service.json
    json -f $ROOT/config/services/loadbalancer/service.json -e " \
        this.name = 'skate-loadbalancer';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${LOADBALANCER_IMAGE_UUID}'; \
        this.metadata = {}; \
        this.metadata.SSL_CERTIFICATE = '${SSL_CERT}'; \
    " >$LOADBALANCER_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$LOADBALANCER_SERVICE_JSON
    LOADBALANCER_SERVICE=$($CURL $SAPI/services?name=skate-loadbalancer | json -Ha uuid)
}

# --- Loadbalancer instance
function loadbalancerInstance() {
    LOADBALANCER_INSTANCE=$($CURL $SAPI/instances?service_uuid=$LOADBALANCER_SERVICE | \
        json -Ha uuid)
    if [[ -n "$LOADBALANCER_INSTANCE" ]]; then
        echo "Skate loadbalancer instance already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-loadbalancer-instance.json.in ]]; do
        sleep 1;
    done

    LOADBALANCER_INSTANCE_JSON=$O/loadbalancer-instance.json
    json -f $DIR/templates/skate-loadbalancer-instance.json.in -e " \
        this.service_uuid = '${LOADBALANCER_SERVICE}'; \
        this.params.server_uuid = '${SERVER_UUID}'; \
    " >$LOADBALANCER_INSTANCE_JSON

    $CURL $SAPI/instances -X POST -d@$LOADBALANCER_INSTANCE_JSON
    LOADBALANCER_INSTANCE=$($CURL $SAPI/instances?service_uuid=$LOADBALANCER_SERVICE | \
        json -Ha uuid)
}

# --- Jobsupervisor service
function jobsupervisorService() {
    JOBSUPERVISOR_SERVICE=$($CURL $SAPI/services?name=skate-jobsupervisor | json -Ha uuid)
    if [[ -n "$JOBSUPERVISOR_SERVICE" ]]; then
        echo "Skate jobsupervisor service already exists."
        return
    fi

    JOBSUPERVISOR_SERVICE_JSON=$O/jobsupervisor-service.json
    json -f $ROOT/config/services/jobsupervisor/service.json -e " \
        this.name = 'skate-jobsupervisor';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${JOBSUPERVISOR_IMAGE_UUID}'; \
    " >$JOBSUPERVISOR_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$JOBSUPERVISOR_SERVICE_JSON
    JOBSUPERVISOR_SERVICE=$($CURL $SAPI/services?name=skate-jobsupervisor | json -Ha uuid)
}

# --- Jobsupervisor instance
function jobsupervisorInstance() {
    JOBSUPERVISOR_INSTANCE=$($CURL $SAPI/instances?service_uuid=$JOBSUPERVISOR_SERVICE | \
        json -Ha uuid)
    if [[ -n "$JOBSUPERVISOR_INSTANCE" ]]; then
        echo "Skate jobsupervisor instance already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-jobsupervisor-instance.json.in ]]; do
        sleep 1;
    done

    JOBSUPERVISOR_INSTANCE_JSON=$O/jobsupervisor-instance.json
    json -f $DIR/templates/skate-jobsupervisor-instance.json.in -e " \
        this.service_uuid = '${JOBSUPERVISOR_SERVICE}'; \
        this.params.server_uuid = '${SERVER_UUID}'; \
    " >$JOBSUPERVISOR_INSTANCE_JSON

    $CURL $SAPI/instances -X POST -d@$JOBSUPERVISOR_INSTANCE_JSON
    JOBSUPERVISOR_INSTANCE=$($CURL $SAPI/instances?service_uuid=$JOBSUPERVISOR_SERVICE | \
        json -Ha uuid)
}

# --- Marlin service
function marlinService() {
    MARLIN_SERVICE=$($CURL $SAPI/services?name=skate-marlin | json -Ha uuid)
    if [[ -n "$MARLIN_SERVICE" ]]; then
        echo "Skate marlin service already exists."
        return
    fi

    MARLIN_SERVICE_JSON=$O/marlin-service.json
    json -f $ROOT/config/services/marlin/service.json -e " \
        this.name = 'skate-marlin';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${MARLIN_IMAGE_UUID}'; \
    " >$MARLIN_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$MARLIN_SERVICE_JSON
    MARLIN_SERVICE=$($CURL $SAPI/services?name=skate-marlin | json -Ha uuid)
}

# --- Marlin instances
function marlinInstances() {
    MARLIN_INSTANCES=$($CURL $SAPI/instances?service_uuid=$MARLIN_SERVICE | \
        json -Ha uuid)
    COUNT=$(echo "$MARLIN_INSTANCES" | wc -w)
    if [[ -n "$MARLIN_INSTANCES" ]] && [[ $COUNT == 2 ]]; then
        echo "Skate marlin instances already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-marlin-instance.json.in ]]; do
        sleep 1;
    done

    while [[ "$COUNT" != "2" ]]; do
        let COUNT=$COUNT+1
        MARLIN_INSTANCE_JSON=$O/marlin-instance-$COUNT.json
        MARLIN_ID=${COUNT}.skate-marlin.${DATACENTER_NAME}.${DNS_DOMAIN}
        json -f $DIR/templates/skate-marlin-instance.json.in -e " \
            this.service_uuid = '${MARLIN_SERVICE}'; \
            this.params.alias = '${MARLIN_ID}'; \
            this.params.server_uuid = '${SERVER_UUID}'; \
        " >$MARLIN_INSTANCE_JSON

        RES=$($CURL $SAPI/instances -X POST -d@$MARLIN_INSTANCE_JSON)
        MARLIN_INSTANCE=$(echo "$RES" | json -Ha uuid)
        CMD="/opt/smartdc/agents/lib/node_modules/marlin/tools/mrdeploycompute \
            ${MARLIN_INSTANCE}"
        $CURL $CNAPI/servers/$SERVER_UUID/execute -X POST \
            -d "{ \"script\": \"$CMD\" }"
    done
}

# --- Marlin Agent
function marlinAgent() {
    # Start Here....
    MANTA_SERVICE=$(json -f $DIR/templates/application.json.in \
        metadata.MANTA_SERVICE)
    MORAY_SHARD=$(json -f $DIR/templates/application.json.in \
        metadata.MARLIN_MORAY_SHARD)
    ZK_SERVER=$(json -f $DIR/templates/application.json.in \
        metadata.ZK_SERVERS.0.host)
    CMD="/opt/smartdc/agents/lib/node_modules/marlin/tools/mragentconf \
        ${COMPUTE_ID} ${MANTA_SERVICE} ${MORAY_SHARD} ${ZK_SERVER}"

    $CURL $CNAPI/servers/$SERVER_UUID/execute -X POST \
        -d "{ \"script\": \"$CMD\" }"
}

# --- Jobpuller service
function jobpullerService() {
    JOBPULLER_SERVICE=$($CURL $SAPI/services?name=skate-jobpuller | json -Ha uuid)
    if [[ -n "$JOBPULLER_SERVICE" ]]; then
        echo "Skate jobpuller service already exists."
        return
    fi

    JOBPULLER_SERVICE_JSON=$O/jobpuller-service.json
    json -f $ROOT/config/services/jobpuller/service.json -e " \
        this.name = 'skate-jobpuller';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${JOBPULLER_IMAGE_UUID}'; \
    " >$JOBPULLER_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$JOBPULLER_SERVICE_JSON
    JOBPULLER_SERVICE=$($CURL $SAPI/services?name=skate-jobpuller | json -Ha uuid)
}

# --- Jobpuller instance
function jobpullerInstance() {
    JOBPULLER_INSTANCE=$($CURL $SAPI/instances?service_uuid=$JOBPULLER_SERVICE | \
        json -Ha uuid)
    if [[ -n "$JOBPULLER_INSTANCE" ]]; then
        echo "Skate jobpuller instance already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-jobpuller-instance.json.in ]]; do
        sleep 1;
    done

    JOBPULLER_INSTANCE_JSON=$O/jobpuller-instance.json
    json -f $DIR/templates/skate-jobpuller-instance.json.in -e " \
        this.service_uuid = '${JOBPULLER_SERVICE}'; \
        this.params.server_uuid = '${SERVER_UUID}'; \
    " >$JOBPULLER_INSTANCE_JSON

    $CURL $SAPI/instances -X POST -d@$JOBPULLER_INSTANCE_JSON
    JOBPULLER_INSTANCE=$($CURL $SAPI/instances?service_uuid=$JOBPULLER_SERVICE | \
        json -Ha uuid)
}

# --- Medusa
function medusaService() {
    MEDUSA_SERVICE=$($CURL $SAPI/services?name=skate-medusa | json -Ha uuid)
    if [[ -n "$MEDUSA_SERVICE" ]]; then
        echo "Skate medusa service already exists."
        return
    fi

    MEDUSA_SERVICE_JSON=$O/medusa-service.json
    json -f $ROOT/config/services/medusa/service.json -e " \
        this.name = 'skate-medusa';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${MEDUSA_IMAGE_UUID}'; \
    " >$MEDUSA_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$MEDUSA_SERVICE_JSON
    MEDUSA_SERVICE=$($CURL $SAPI/services?name=skate-medusa | json -Ha uuid)
}

# --- Medusa instance
function medusaInstance() {
    MEDUSA_INSTANCE=$($CURL $SAPI/instances?service_uuid=$MEDUSA_SERVICE | \
        json -Ha uuid)
    if [[ -n "$MEDUSA_INSTANCE" ]]; then
        echo "Skate medusa instance already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-medusa-instance.json.in ]]; do
        sleep 1;
    done

    MEDUSA_INSTANCE_JSON=$O/medusa-instance.json
    json -f $DIR/templates/skate-medusa-instance.json.in -e " \
        this.service_uuid = '${MEDUSA_SERVICE}'; \
        this.params.server_uuid = '${SERVER_UUID}'; \
    " >$MEDUSA_INSTANCE_JSON

    $CURL $SAPI/instances -X POST -d@$MEDUSA_INSTANCE_JSON
    MEDUSA_INSTANCE=$($CURL $SAPI/instances?service_uuid=$MEDUSA_SERVICE | \
        json -Ha uuid)
}

# --- Ops
function opsService() {
    OPS_SERVICE=$($CURL $SAPI/services?name=skate-ops | json -Ha uuid)
    if [[ -n "$OPS_SERVICE" ]]; then
        echo "Skate ops service already exists."
        return
    fi

    OPS_SERVICE_JSON=$O/ops-service.json
    json -f $ROOT/config/services/ops/service.json -e " \
        this.name = 'skate-ops';
        this.application_uuid = '${SKATE_APP}'; \
        this.params.networks = [ 'admin' ]; \
        this.params.image_uuid = '${OPS_IMAGE_UUID}'; \
    " >$OPS_SERVICE_JSON

    $CURL $SAPI/services -X POST -d@$OPS_SERVICE_JSON
    OPS_SERVICE=$($CURL $SAPI/services?name=skate-ops | json -Ha uuid)
}

# --- Ops instance
function opsInstance() {
    OPS_INSTANCE=$($CURL $SAPI/instances?service_uuid=$OPS_SERVICE | \
        json -Ha uuid)
    if [[ -n "$OPS_INSTANCE" ]]; then
        echo "Skate ops instance already exists."
        return
    fi

    while [[ ! -f $DIR/templates/skate-ops-instance.json.in ]]; do
        sleep 1;
    done

    OPS_INSTANCE_JSON=$O/ops-instance.json
    json -f $DIR/templates/skate-ops-instance.json.in -e " \
        this.service_uuid = '${OPS_SERVICE}'; \
        this.params.server_uuid = '${SERVER_UUID}'; \
    " >$OPS_INSTANCE_JSON

    $CURL $SAPI/instances -X POST -d@$OPS_INSTANCE_JSON
    OPS_INSTANCE=$($CURL $SAPI/instances?service_uuid=$OPS_SERVICE | \
        json -Ha uuid)
}

bootstrapConfigs
skateApplication
storService
storInstances
webapiService
webapiInstance
loadbalancerService
loadbalancerInstance
jobsupervisorService
jobsupervisorInstance
jobpullerService
jobpullerInstance
marlinService
marlinInstances
marlinAgent
medusaService
medusaInstance
opsService
opsInstance
