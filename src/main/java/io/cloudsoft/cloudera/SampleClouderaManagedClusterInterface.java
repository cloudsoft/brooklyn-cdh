package io.cloudsoft.cloudera;

import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.Description;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.NamedParameter;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;

@ImplementedBy(SampleClouderaManagedCluster.class)
public interface SampleClouderaManagedClusterInterface extends StartableApplication {

    public static final ConfigKey<Boolean> SETUP_DNS = ConfigKeys.newBooleanConfigKey("cdh.setupDns", "Whether to set up internal DNS", true);

    MethodEffector<Void> START_SERVICES = new MethodEffector<Void>(
            SampleClouderaManagedClusterInterface.class, "startServices");

    @Description("Start the Cloudera services")
    public void startServices(
            @NamedParameter("isCertificationCluster") boolean isCertificationCluster,
            @NamedParameter("includeHbase") boolean includeHbase);

    public static final AttributeSensor<String> CLOUDERA_MANAGER_URL = ClouderaManagerNode.CLOUDERA_MANAGER_URL;
    
    public void launchDefaultServices(boolean enabled);

}
