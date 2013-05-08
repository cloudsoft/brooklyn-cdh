package io.cloudsoft.cloudera.brooklynnodes;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.util.flags.SetFromFlag;


public interface ClouderaManagerNode extends Entity {

    public static final Logger log = LoggerFactory.getLogger(ClouderaManagerNode.class);

    @SetFromFlag("name")
    public static final ConfigKey<String> NAME = new BasicConfigKey<String>(
    		String.class, "cloudera.manager.name", "The name of the CM cluster");

    public static final AttributeSensor<String> CLOUDERA_MANAGER_HOSTNAME = new BasicAttributeSensor<String>(
    		String.class, "cloudera.manager.hostname.public", "Public hostname for the Cloudera Manager node");

    public static final AttributeSensor<String> CLOUDERA_MANAGER_URL = new BasicAttributeSensor<String>(
    		String.class, "cloudera.manager.url.public", "URL for the Cloudera Manager node");
        
    public static final AttributeSensor<List> MANAGED_HOSTS = new BasicAttributeSensor<List>(
    		List.class, "cloudera.manager.hosts", "List of hosts managed by this CM instance");

    public static final AttributeSensor<List> MANAGED_CLUSTERS = new BasicAttributeSensor<List>(
    		List.class, "clouder.manager.clusters", "List of clusters managed by this CM instance");

    public ClouderaCdhNode findEntityForHostId(String hostId);
    
}
