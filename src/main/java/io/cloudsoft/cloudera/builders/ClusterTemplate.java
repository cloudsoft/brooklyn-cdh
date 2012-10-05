package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;

import java.util.List;

import brooklyn.util.text.Identifiers;

public class ClusterTemplate extends AbstractTemplate<ClusterTemplate> {

    boolean skipIfExists = false;
    public AbstractTemplate<ClusterTemplate> skipIfExists() {
        skipIfExists = true;
        return this;
    }

    @Override
    public String build(ClouderaRestCaller caller) {
        List<String> clusters = caller.getClusters();
        if (name==null) name = "cluster-"+Identifiers.makeRandomId(8);
        if (clusters.contains(name) && skipIfExists) return name;
        caller.addCluster(name);
        return name;
    }

    public static void main(String[] args) {
        String SERVER = "ec2-23-22-170-157.compute-1.amazonaws.com";
        ClouderaRestCaller caller = ClouderaRestCaller.newInstance(SERVER, "admin", "admin");

        new ClusterTemplate().
                named("foo-bar").
                skipIfExists().
                build(caller);
    }
}
