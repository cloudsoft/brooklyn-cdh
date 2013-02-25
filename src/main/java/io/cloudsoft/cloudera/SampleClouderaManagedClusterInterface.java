package io.cloudsoft.cloudera;

import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;

@ImplementedBy(SampleClouderaManagedCluster.class)
public interface SampleClouderaManagedClusterInterface extends StartableApplication {

    public static final AttributeSensor<String> CLOUDERA_MANAGER_URL = ClouderaManagerNode.CLOUDERA_MANAGER_URL;
    
    public void launchDefaultServices(boolean enabled);
    public void startServices(boolean isCertificationCluster, boolean includeHbase);
    
}
