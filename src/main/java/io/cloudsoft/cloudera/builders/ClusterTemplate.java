package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaApi;

import java.util.List;

import com.cloudera.api.model.ApiCluster;

import brooklyn.util.text.Identifiers;

public class ClusterTemplate extends AbstractTemplate<ClusterTemplate> {

    boolean skipIfExists = false;
    public AbstractTemplate<ClusterTemplate> skipIfExists() {
        skipIfExists = true;
        return this;
    }

    @Override
    public String build(ClouderaApi api) {
        List<ApiCluster> clusters = api.listClusters();
        if (name==null) name = "cluster-"+Identifiers.makeRandomId(8);
        if (clusters.contains(name) && skipIfExists) return name;
        api.addCluster(name);
        return name;
    }

}
