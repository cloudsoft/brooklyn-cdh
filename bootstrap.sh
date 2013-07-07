#!/bin/bash -x

. ./setup.sh

JAVA_OPTS="-Xms256m -Xmx1g -XX:MaxPermSize=256m"
JAVA_OPTS="$JAVA_OPTS -Dcdh.vcloudCredential=XXXXXXX -Dcdh.vcloudIdentity=cloudsoftcorp@paas -Dcdh.vcloudEndpoint=https://vcloud.octocloud.org"
JAVA_OPTS="$JAVA_OPTS -Dbrooklyn.downloadUrl=/home/cloudsoft/brooklyn-0.6.0-SNAPSHOT-dist-aled-20130707-1510.tar.gz"
JAVA_OPTS="$JAVA_OPTS -Dbrooklyn.uploadUrl="
export JAVA_OPTS

brooklyn launch --app io.cloudsoft.bootstrap.ClouderaBootstrapLauncher --location "byon:(hosts=cloudsoft@62.30.101.215)"

