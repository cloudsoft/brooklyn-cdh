package io.cloudsoft.cloudera;

import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.proxying.ImplementedBy;

@ImplementedBy(SampleClouderaManagedCluster.class)
public interface SampleClouderaManagedClusterInterface extends StartableApplication {

    public void launchDefaultServices(boolean enabled);
    public void startServices(boolean isCertificationCluster, boolean includeHbase);
    
}
