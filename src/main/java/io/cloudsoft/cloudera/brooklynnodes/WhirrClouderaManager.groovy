package io.cloudsoft.cloudera.brooklynnodes;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.proxying.ImplementedBy
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.extras.whirr.core.WhirrCluster
import brooklyn.util.flags.SetFromFlag


@ImplementedBy(WhirrClouderaManagerImpl.class)
public interface WhirrClouderaManager extends WhirrCluster, ClouderaManagerNode {

    public static final Logger log = LoggerFactory.getLogger(WhirrClouderaManager.class);

    @SetFromFlag("memory")
    public static final BasicConfigKey<Integer> MEMORY =
        [Integer, "cloudera.manager.memory.whirr", "The minimum amount of memory to use for the CM node (in megabytes)", 2560]
    
}
