package io.cloudsoft.cloudera;

import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;

@ImplementedBy(SampleClouderaManagedCluster.class)
public interface SampleClouderaManagedClusterInterface extends StartableApplication {

    MethodEffector<Void> START_SERVICES = new MethodEffector<Void>(
            SampleClouderaManagedClusterInterface.class, "startServices");

    @Effector(description="Start the Cloudera services")
    public void startServices(
            @EffectorParam(name="isCertificationCluster") boolean isCertificationCluster,
            @EffectorParam(name="includeHbase") boolean includeHbase);

    public static final AttributeSensor<String> CLOUDERA_MANAGER_URL = ClouderaManagerNode.CLOUDERA_MANAGER_URL;
    
    public void launchDefaultServices(boolean enabled);

}
