#!/bin/bash
# Script to start a Cloudera managed cluster using AMP.
#
# Defaults to using a pre-configured location in brooklyn.properties named 'cloudera', otherwise
# uses the first argument as the required location and passes the rest to AMP unchanged. Must
# be run from the directory the script and associated assets were installed into.
#
# Usage:
#     ./start.sh [ [location] <argument list for AMP CLI> ]
#
# Examples:
#     ./start.sh
#     ./start.sh aws-ec2:eu-west-1
#     ./start.sh named:test
#     ./start.sh rackspace-cloudservers-uk --port 8000
#
# Copyright 2012-2013 by Cloudsoft Corp.

#set -x # DEBUG

JAVA=$(which java)
if [ ! -z "$JAVA_HOME" ] ; then
    JAVA=$JAVA_HOME/bin/java
fi
if [ ! -x "$JAVA" ] ; then
  echo Cannot find java. Set JAVA_HOME or add java to path.
  exit 1
fi

if [ ! $(ls brooklyn-cdh-*.jar 2> /dev/null) ] ; then
  echo Command must be run from the directory where it is installed.
  exit 1
fi

if [ $# -eq 0 ] ; then
    location=named:cloudera
else
    location=$1
    shift
fi

$JAVA -Xms256m -Xmx1024m -XX:MaxPermSize=1024m -Djclouds.ssh.max-retries=100 \
        -Djclouds.so-timeout=120000 -Djclouds.connection-timeout=120000 \
        -classpath "*:lib/*" io.cloudsoft.cloudera.SampleClouderaManagedCluster \
        --location ${location} "$@"
