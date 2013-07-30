package io.cloudsoft.cloudera;

import brooklyn.catalog.CatalogConfig;
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

   @CatalogConfig(label="Number of CDH nodes", priority=2)
   public static final ConfigKey<Integer> INITIAL_SIZE_NODES = ConfigKeys.newIntegerConfigKey("cdh.initial.node.count", 
           "Number of CDH nodes to deploy initially", 4);

   @CatalogConfig(label="Certification cluster (metrics enabled)", priority=4)
   public static final ConfigKey<Boolean> DEPLOY_CERTIFICATION_CLUSTER = ConfigKeys.newBooleanConfigKey("cdh.certificationCluster",
           "Whether to deploy a certification cluster, i.e. enable metrics collection", true);
   
   @CatalogConfig(label="Deploy HBase", priority=4)
   public static final ConfigKey<Boolean> DEPLOY_HBASE = ConfigKeys.newBooleanConfigKey("cdh.initial.services.hbase",
           "Whether to deploy HBase as part of initial services roll-out", false);
   
    MethodEffector<Void> START_SERVICES = new MethodEffector<Void>(
            SampleClouderaManagedClusterInterface.class, "startServices");

    @Description("Start the Cloudera services")
    public void startServices(
            @NamedParameter("isCertificationCluster") boolean isCertificationCluster,
            @NamedParameter("includeHbase") boolean includeHbase);

    public static final AttributeSensor<String> CLOUDERA_MANAGER_URL = ClouderaManagerNode.CLOUDERA_MANAGER_URL;
    
    public void launchDefaultServices(boolean enabled);

}
