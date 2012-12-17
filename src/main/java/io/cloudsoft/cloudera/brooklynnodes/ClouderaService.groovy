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
import brooklyn.entity.trait.Startable
import brooklyn.event.adapter.FunctionSensorAdapter
import brooklyn.event.adapter.SensorRegistry
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.location.Location
import brooklyn.util.flags.SetFromFlag

public class ClouderaService extends AbstractEntity implements Startable {

    private static final Logger log = LoggerFactory.getLogger(ClouderaService.class);
    
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
    static {
        RendererHints.register(SERVICE_URL, new RendererHints.NamedActionWithUrl("Open"));
    }
    
    SensorRegistry sensorRegistry;
    
    ClouderaRestCaller getApi() {
        return new ClouderaRestCaller(server: getConfig(MANAGER).getAttribute(WhirrClouderaManager.CLOUDERA_MANAGER_HOSTNAME), authName:"admin", authPass: "admin");
    }
    
    public ClouderaService(Map flags=[:], Entity owner=null) { super(flags, owner); }
    public ClouderaService(Entity owner) { this([:], owner); }

    @Description("Start the process/service represented by an entity")
    void create() {
        if (getAttribute(SERVICE_STATE)!=null) {
            throw new IllegalStateException("${this} is already created");
        }
        setAttribute(SERVICE_STATE, Lifecycle.STARTING);
        
        log.info("Creating CDH service ${this}");
        Boolean buildResult = getConfig(TEMPLATE).build(getApi());
        log.info("Created CDH service ${this}, result: "+buildResult);
        
        setAttribute(SERVICE_STATE, buildResult ? Lifecycle.RUNNING : Lifecycle.ON_FIRE);
        setAttribute(SERVICE_NAME, getConfig(TEMPLATE).getName());
        setAttribute(CLUSTER_NAME, getConfig(TEMPLATE).getClusterName());
        connectSensors();
    }
    
    protected void connectSensors() {
        if (!sensorRegistry) sensorRegistry = new SensorRegistry(this);
        
        FunctionSensorAdapter fnSensorAdaptor = sensorRegistry.register(new FunctionSensorAdapter({ getApi() }, period: 30*TimeUnit.SECONDS));
        fnSensorAdaptor.poll(SERVICE_REGISTERED, { it.getServices(getClusterName()).contains(getServiceName()) });
        def roles = fnSensorAdaptor.then({ it.getServiceRolesJson(getClusterName(), getServiceName()) });
        roles.poll(HOSTS) { it.items.collect { it.hostRef.hostId } }
        roles.poll(ROLES) { (it.items.collect { it.type }) as Set }
        def state = fnSensorAdaptor.then({ it.getServiceJson(getClusterName(), getServiceName()) });
        state.poll(SERVICE_UP, { it.serviceState == "STARTED" });
        state.poll(SERVICE_HEALTH, { it.healthSummary });
        state.poll(SERVICE_URL, { it.serviceUrl });
        
        sensorRegistry.activateAdapters();
    }
    
    String getClusterName() { return getAttribute(CLUSTER_NAME); }
    String getServiceName() { return getAttribute(SERVICE_NAME); }
    
    void invokeServiceCommand(String cmd) {
        Object result = getApi().invokeServiceCommand(clusterName, serviceName, cmd).block(5*60*1000);
        if (!result) {
            setAttribute(SERVICE_STATE, Lifecycle.ON_FIRE);
            throw new IllegalStateException("The service failed to ${cmd}.");
        }
    }

    @Override
    @Description("Start the process/service represented by an entity")
    void start(@NamedParameter("locations") Collection<? extends Location> locations) {
        if (locations) log.debug("Ignoring locations at ${this}");
        if (getAttribute(SERVICE_STATE)==Lifecycle.RUNNING) {
            log.debug("Ignoring start when already started at ${this}");
            return;
        }
        setAttribute(SERVICE_STATE, Lifecycle.STARTING);
        invokeServiceCommand("start");
        setAttribute(SERVICE_STATE, Lifecycle.RUNNING);
    }

    
    @Override
    @Description("Stop the process/service represented by an entity")
    void stop() {
        if (getAttribute(SERVICE_STATE)==Lifecycle.STOPPED) {
            log.debug("Ignoring stop when already stopped at ${this}");
            return;
        }
        setAttribute(SERVICE_STATE, Lifecycle.STOPPING);
        invokeServiceCommand("stop");
        setAttribute(SERVICE_STATE, Lifecycle.STOPPED);
    }

    @Override
    @Description("Restart the process/service represented by an entity")
    void restart() {
        setAttribute(SERVICE_STATE, Lifecycle.STARTING);
        invokeServiceCommand("restart");
        setAttribute(SERVICE_STATE, Lifecycle.RUNNING);
    }

}
