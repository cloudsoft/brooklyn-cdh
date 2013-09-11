#!/bin/bash

JAVA=`which java`

if [ ! -z "$JAVA_HOME" ] ; then 
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=`which java`
fi

if [ ! -x "$JAVA" ] ; then
  echo Cannot find java. Set JAVA_HOME or add java to path.
  exit 1
fi

if [[ "$1" == --location ]] ; then
  # this script assumes location is the first argument
  shift
fi

if [[ ! `ls brooklyn-cdh-*.jar 2> /dev/null` ]] ; then
  echo Command must be run from the directory where it is installed.
  exit 1
fi

$JAVA -Xms256m -Xmx1024m -XX:MaxPermSize=1024m -Djclouds.ssh.max-retries=100 -Djclouds.so-timeout=120000 -Djclouds.connection-timeout=120000 -classpath "*:lib/*" io.cloudsoft.cloudera.SampleClouderaManagedCluster --location $1 "$@"
