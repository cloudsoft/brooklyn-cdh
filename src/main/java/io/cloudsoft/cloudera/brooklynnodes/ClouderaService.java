package io.cloudsoft.cloudera.brooklynnodes;

import io.cloudsoft.cloudera.builders.ServiceTemplate;
import io.cloudsoft.cloudera.rest.ClouderaRestCaller;

import java.util.Collection;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.entity.trait.Startable;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.util.flags.SetFromFlag;


@ImplementedBy(ClouderaServiceImpl.class)
public interface ClouderaService extends Entity, Startable {

    @SetFromFlag(value="manager", nullable=false)
    public static final ConfigKey<ClouderaManagerNode> MANAGER = new BasicConfigKey<ClouderaManagerNode>(
        	ClouderaManagerNode.class, "cloudera.cdh.node.manager", "Cloudera Manager entity");

    @SetFromFlag(value="template", nullable=false)
    public static final ConfigKey<ServiceTemplate> TEMPLATE = new BasicConfigKey<ServiceTemplate>(
    		ServiceTemplate.class, "cloudera.service.template", "Template for service to start");

    public static final AttributeSensor<String> CLUSTER_NAME = new BasicAttributeSensor<String>(
    		String.class, "cloudera.service.cluster.name", "Name of cluster where this service runs");

    public static final AttributeSensor<String> SERVICE_NAME = new BasicAttributeSensor<String>(
       		String.class, "cloudera.service.name", "Name of this service, as known by the manager");

    public static final AttributeSensor<Boolean> SERVICE_REGISTERED = new BasicAttributeSensor<Boolean>(
        	Boolean.class, "cloudera.service.registered", "Whether this service is registered (ie known by the manager)");

    public static final AttributeSensor<Lifecycle> SERVICE_STATE = Attributes.SERVICE_STATE;

    public static final AttributeSensor<String> SERVICE_HEALTH = new BasicAttributeSensor<String>(
    		String.class, "cloudera.service.health", "Health of service, as reported by the manager");

    public static final AttributeSensor<Collection<String>> HOSTS = new BasicAttributeSensor(
        	Collection.class, "cloudera.service.hosts", "Hosts involved in this service");

    public static final AttributeSensor<Collection<String>> ROLES = new BasicAttributeSensor(
        	Collection.class, "cloudera.service.roles", "Roles involved in this service");

    public static final AttributeSensor<String> SERVICE_URL = new BasicAttributeSensor<String>(
        	String.class, "cloudera.service.url", "Direct URL of service (may not be accessible if in private subnet)");
        
    @Effector(description="Start the process/service represented by an entity")
    public void create();

    public ClouderaRestCaller getApi();

}
