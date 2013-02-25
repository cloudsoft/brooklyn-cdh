package io.cloudsoft.cloudera.brooklynnodes;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.proxying.ImplementedBy
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.extras.whirr.core.WhirrCluster
import brooklyn.util.flags.SetFromFlag


@ImplementedBy(WhirrClouderaManagerImpl.class)
public interface WhirrClouderaManager extends WhirrCluster {

    public static final Logger log = LoggerFactory.getLogger(WhirrClouderaManager.class);

    @SetFromFlag("name")
    public static final BasicConfigKey<String> NAME =
        [String, "whirr.cm.name", "The name of the CM cluster"]

    @SetFromFlag("memory")
    public static final BasicConfigKey<Integer> MEMORY =
        [Integer, "whirr.cm.memory", "The minimum amount of memory to use for the CM node (in megabytes)", 2560]

    public static final BasicAttributeSensor<String> CLOUDERA_MANAGER_HOSTNAME =
        [String, "whirr.cm.hostname", "Public hostname for the Cloudera Manager node"]

    public static final BasicAttributeSensor<String> CLOUDERA_MANAGER_URL =
        [String, "whirr.cm.url", "URL for the Cloudera Manager node"];
        
//    static {
//        RendererHints.register(CLOUDERA_MANAGER_URL, new RendererHints.NamedActionWithUrl("Open"));
//    }

    public static final BasicAttributeSensor<List> MANAGED_HOSTS =
        [List, "whirr.cm.hosts", "List of hosts managed by this CM instance"]

    public static final BasicAttributeSensor<List> MANAGED_CLUSTERS =
        [List, "whirr.cm.clusters", "List of clusters managed by this CM instance"]

    public ClouderaCdhNode findEntityForHostId(String hostId);
    
}
