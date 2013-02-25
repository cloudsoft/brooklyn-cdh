package io.cloudsoft.cloudera.brooklynnodes;

import io.cloudsoft.cloudera.builders.ServiceTemplate
import io.cloudsoft.cloudera.rest.ClouderaRestCaller

import java.util.concurrent.TimeUnit

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.config.render.RendererHints
import brooklyn.entity.Entity
import brooklyn.entity.basic.AbstractEntity
import brooklyn.entity.basic.Attributes
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.Lifecycle
import brooklyn.entity.basic.NamedParameter
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.entity.trait.Startable
import brooklyn.event.adapter.FunctionSensorAdapter
import brooklyn.event.adapter.SensorRegistry
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.location.Location
import brooklyn.util.flags.SetFromFlag


@ImplementedBy(ClouderaServiceImpl.class)
public interface ClouderaService extends Entity, Startable {

    @SetFromFlag(value="manager", nullable=false)
    public static final BasicConfigKey<WhirrClouderaManager> MANAGER =
        [WhirrClouderaManager.class, "cloudera.cdh.node.manager", "Cloudera Manager entity"];

    @SetFromFlag(value="template", nullable=false)
    public static final BasicConfigKey<ServiceTemplate> TEMPLATE =
        [ServiceTemplate, "cloudera.service.template", "Template for service to start"]

    public static final BasicAttributeSensor<String> CLUSTER_NAME =
        [String, "cloudera.service.cluster.name", "Name of cluster where this service runs"]

    public static final BasicAttributeSensor<String> SERVICE_NAME =
        [String, "cloudera.service.name", "Name of this service, as known by the manager"]

    public static final BasicAttributeSensor<Boolean> SERVICE_REGISTERED =
        [Boolean, "cloudera.service.registered", "Whether this service is registered (ie known by the manager)"]

    public static final BasicAttributeSensor<Lifecycle> SERVICE_STATE = Attributes.SERVICE_STATE;

    public static final BasicAttributeSensor<String> SERVICE_HEALTH =
        [String, "cloudera.service.health", "Health of service, as reported by the manager"]

    public static final BasicAttributeSensor<Collection<String>> HOSTS =
        [Collection, "cloudera.service.hosts", "Hosts involved in this service"]

    public static final BasicAttributeSensor<Collection<String>> ROLES =
        [Collection, "cloudera.service.roles", "Roles involved in this service"]

    public static final BasicAttributeSensor<String> SERVICE_URL =
        [String, "cloudera.service.url", "Direct URL of service (may not be accessible if in private subnet)"]
        
    @Description("Start the process/service represented by an entity")
    public void create();

    public ClouderaRestCaller getApi();

}
