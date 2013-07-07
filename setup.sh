#!/bin/bash

export REPOS_DIR=/Users/aled/repos
export M2_REPOS_DIR=/Users/aled/.m2/repository
export BROOKLYN_HOME=$REPOS_DIR/brooklyncentral/brooklyn/usage/dist/target/brooklyn-dist

export PATH=$BROOKLYN_HOME/bin:$PATH

BROOKLYN_CLASSPATH=$BROOKLYN_CLASSPATH:$M2_REPOS_DIR/io/cloudsoft/amp/locations/vcloud-director/0.6.0-SNAPSHOT/vcloud-director-0.6.0-SNAPSHOT.jar
BROOKLYN_CLASSPATH=$BROOKLYN_CLASSPATH:$M2_REPOS_DIR/io/cloudsoft/amp/locations/ibm-smartcloud/0.6.0-SNAPSHOT/ibm-smartcloud-0.6.0-SNAPSHOT.jar
BROOKLYN_CLASSPATH=$BROOKLYN_CLASSPATH:$REPOS_DIR/cloudsoft/brooklyn-cdh/target/brooklyn-cdh-1.1.0-SNAPSHOT.jar
BROOKLYN_CLASSPATH=$BROOKLYN_CLASSPATH:$M2_REPOS_DIR/com/vmware/vcloud/vcloud-java-sdk/5.1.0/vcloud-java-sdk-5.1.0.jar
BROOKLYN_CLASSPATH=$BROOKLYN_CLASSPATH:$M2_REPOS_DIR/com/vmware/vcloud/rest-api-schemas/5.1.0/rest-api-schemas-5.1.0.jar
export BROOKLYN_CLASSPATH
