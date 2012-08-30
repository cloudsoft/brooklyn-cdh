brooklyn-cdh
============

Brooklyn deployment and management of Cloudera Hadoop and Manager clusters.


### Setup 1:  Dependencies

You must have the following software installed and compiled (`mvn clean install`):

* `https://github.com/brooklyncentral/brooklyn`: currently snapshot is required, and 
  below we assume that `brooklyn` is in your path 
* `https://github.com/ahgittin/whirr-cm`: below we assume this is in a sibling directory to `brooklyn-cdh` 


### Compile

To compile brooklyn-cdh, simply `mvn clean install` in the project root.


### Setup 2:  Credentials

To run, you'll need AWS credentials in `~/.brooklyn/brooklyn.properties`:

    brooklyn.jclouds.aws-ec2.identity=AKXXXXXXXXXXXXXXXXXX
    brooklyn.jclouds.aws-ec2.credential=secret01xxxxxxxxxxxxxxxxxxxxxxxxxxx

Most other clouds should work too; just provide the keys like above, and change the `-l` argument below.


### Run

To run it, either:

* Install the `brooklyn` CLI tool, either from source (as above) or from 
  http://brooklyncentral.github.com/ and then in root of this project:

        export BROOKLYN_CLASSPATH=target/brooklyn-cdh-0.0.1-SNAPSHOT.jar:export BROOKLYN_CLASSPATH=target/brooklyn-cdh-0.0.1-SNAPSHOT.jar:../whirr-cm/target/whirr-cm-1.1-SNAPSHOT.jar
        brooklyn launch -a io.cloudsoft.cloudera.SampleClouderaManagedCluster -l aws-ec2:us-east-1

* Grab all dependencies (using maven, or in your favourite IDE) and run the 
  static `main` in `io.cloudsoft.cloudera.SampleClouderaManagedCluster`.

After about 15 minutes, it should print out the URL of the Cloudera Manager node.
In the meantime you can follow the progress in the Brooklyn console, 
usually at localhost:8081.  


### Limitations and WIP

Currently we only have automation for deploying a limited set of services.
We are working on others, and on the best representation for services in the
Brooklyn GUI and in the programmatic API.


### Finally

This software is (c) 2012 Cloudsoft Corporation, released as open source under the Apache License v2.0.

Any questions drop a line to brooklyn-users@googlegroups.com .

