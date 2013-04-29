package io.cloudsoft.cloudera.brooklynnodes;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.Entity
import brooklyn.entity.trait.Startable
import brooklyn.event.AttributeSensor
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.event.basic.PortAttributeSensorAndConfigKey;
import brooklyn.location.basic.PortRanges;
import brooklyn.util.flags.SetFromFlag


public interface ClouderaManagerNode extends Entity {

    public static final Logger log = LoggerFactory.getLogger(ClouderaManagerNode.class);

    @SetFromFlag("name")
    public static final BasicConfigKey<String> NAME =
        [String, "cloudera.manager.name", "The name of the CM cluster"]

    public static final BasicAttributeSensor<String> CLOUDERA_MANAGER_HOSTNAME =
        [String, "cloudera.manager.hostname.public", "Public hostname for the Cloudera Manager node"]

    public static final BasicAttributeSensor<String> CLOUDERA_MANAGER_URL =
        [String, "cloudera.manager.url.public", "URL for the Cloudera Manager node"];
        
    public static final BasicAttributeSensor<List> MANAGED_HOSTS =
        [List, "cloudera.manager.hosts", "List of hosts managed by this CM instance"]

    public static final BasicAttributeSensor<List> MANAGED_CLUSTERS =
        [List, "cloudera.manager.clusters", "List of clusters managed by this CM instance"]

    public ClouderaCdhNode findEntityForHostId(String hostId);
    
}
