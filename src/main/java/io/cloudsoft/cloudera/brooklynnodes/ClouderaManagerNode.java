package io.cloudsoft.cloudera.brooklynnodes;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.event.basic.Sensors;
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
    		List.class, "cloudera.manager.clusters", "List of clusters managed by this CM instance");
    
    public static final BasicConfigKey<String> APT_PROXY = 
        new BasicConfigKey<String>(String.class, "cloudera.manager.apt.proxy.url", "URL of apt proxy");
    
    public static final ConfigKey<String> YUM_MIRROR = 
            new BasicConfigKey<String>(String.class, "cloudera.manager.yum.mirror.url", "URL of yum mirror");

    public static final BasicConfigKey<Boolean> USE_IP_ADDRESS = 
        new BasicConfigKey<Boolean>(Boolean.class, "cloudera.manager.use.ip.address", "Force ip address usage instead of hostname", false);

    public static final AttributeSensor<String> LOCAL_HOSTNAME = Sensors.newStringSensor( "host.localName", "Host name, according to `hostname` on the machine");
    
    public ClouderaCdhNode findEntityForHostId(String hostId);
    
}
