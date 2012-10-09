brooklyn-cdh
============

Brooklyn deployment and management of Cloudera Hadoop and Manager clusters.

To use, configure your cloud credentials then run  ./start.sh  in this directory.
You can then access the management context in your browser, typically on  localhost:8081.


### Cloud Credentials

To run, you'll need to specify credentials for your preferred cloud.  This can be done 
in `~/.brooklyn/brooklyn.properties`:

    brooklyn.jclouds.aws-ec2.identity=AKXXXXXXXXXXXXXXXXXX
    brooklyn.jclouds.aws-ec2.credential=secret01xxxxxxxxxxxxxxxxxxxxxxxxxxx

Alternatively these can be set as shell environment parameters or JVM system properties.

Many other clouds are supported also, as well as pre-existing machines ("bring your own nodes"),
custom endpoints for private clouds, and specifying custom keys and passphrases.
For more information see:

    https://github.com/brooklyncentral/brooklyn/blob/master/docs/use/guide/defining-applications/common-usage.md#off-the-shelf-locations


### Run

Usage:

    ./start.sh [--port 8081+] location

Where location might be `aws-ec2:us-east-1` (the default), `gogrid`, `openstack:endpoint`, etc.

After about 15 minutes, it should print out the URL of the Cloudera Manager node.
In the meantime you can follow the progress in the Brooklyn console, 
usually at localhost:8081 (unless a specific port is given).


### More About Brooklyn

Brooklyn is a code library and framework for managing distributed applications
in the cloud.  It has been used to create this project for rolling out Cloudera,
building on Whirr and other routines.

This project can be extended for more complex topologies, additional applications
which run alongside CDH, and to develop sophisticated management policies to
scale or tune the cluster for specific applications.

For more information consider:

* Visiting the open-source Brooklyn home page at  http://brooklyncentral.github.com
* Forking the Brooklyn project at  http://github.com/brooklyncentral/brooklyn
* Forking the brooklyn-cdh project at  http://github.com/cloudsoft/brooklyn-cdh
* Emailing  brooklyn-users@googlegroups.com 

For commercial enquiries -- including bespoke development and paid support --
contact Cloudsoft, the supporters of Brooklyn, at:

* www.CloudsoftCorp.com
* info@cloudsoftcorp.com

This software is (c) 2012 Cloudsoft Corporation, released as open source under the Apache License v2.0.

