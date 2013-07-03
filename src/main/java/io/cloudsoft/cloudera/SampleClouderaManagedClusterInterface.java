package io.cloudsoft.cloudera;

import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;
import brooklyn.catalog.CatalogConfig;
import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.location.vmware.vcloud.director.VCloudDirectorLocationConfig;

@ImplementedBy(SampleClouderaManagedCluster.class)
public interface SampleClouderaManagedClusterInterface extends StartableApplication {

    public static final ConfigKey<Boolean> SETUP_DNS = ConfigKeys.newBooleanConfigKey("cdh.setupDns", "Whether to set up internal DNS", true);

    @CatalogConfig(label="Cluster Size")
    public static final ConfigKey<Integer> CLUSTER_SIZE = ConfigKeys.newIntegerConfigKey("cdh.clusterSize", "Number of worker-nodes in the CDH cluster", 3);

    @CatalogConfig(label="CPU Count")
    public static final ConfigKey<Integer> CPU_COUNT = VCloudDirectorLocationConfig.CPU_COUNT;

    @CatalogConfig(label="Memory (MB)")
    public static final ConfigKey<Double> MEMORY_SIZE_MB = VCloudDirectorLocationConfig.MEMORY_SIZE_MB;

    public static final AttributeSensor<String> CLOUDERA_MANAGER_URL = ClouderaManagerNode.CLOUDERA_MANAGER_URL;
    
    public void launchDefaultServices(boolean enabled);
    public void startServices(boolean isCertificationCluster, boolean includeHbase);
    
}
