package io.cloudsoft.cloudera.brooklynnodes;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller

import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.config.render.RendererHints
import brooklyn.entity.Entity
import brooklyn.entity.basic.AbstractEntity
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.Lifecycle
import brooklyn.entity.basic.NamedParameter
import brooklyn.event.feed.function.FunctionFeed
import brooklyn.event.feed.function.FunctionPollConfig
import brooklyn.location.Location

import com.google.common.base.Functions

public class ClouderaServiceImpl extends AbstractEntity implements ClouderaService {

    private static final Logger log = LoggerFactory.getLogger(ClouderaServiceImpl.class);
        
    static {
        RendererHints.register(ClouderaService.SERVICE_URL, new RendererHints.NamedActionWithUrl("Open"));
    }

    ClouderaRestCaller getApi() {
        return new ClouderaRestCaller(server: getConfig(MANAGER).getAttribute(ClouderaManagerNode.CLOUDERA_MANAGER_HOSTNAME), authName:"admin", authPass: "admin");
    }
    
    public ClouderaServiceImpl(Map flags=[:], Entity owner=null) { super(flags, owner); }
    public ClouderaServiceImpl(Entity owner) { this([:], owner); }

    public void create() {
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
        /*
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
        */
        FunctionFeed feed = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_REGISTERED)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            try {
                                return (it.getServices(getClusterName()).contains(getServiceName()));
                            }
                            catch (Exception e) {
                                return false;
                            }
                        }
                    })
                    .onError(Functions.constant(false))
                    )
                .poll(new FunctionPollConfig<List,List>(HOSTS)
                .period(30, TimeUnit.SECONDS)
                .callable(new Callable<List>() {
                    @Override
                    public List call() throws Exception {
                        return it.items.collect { it.hostRef.hostId };
                    }
                })
                .onError(Functions.constant(false))
                )
                .poll(new FunctionPollConfig<List,List>(ROLES)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<Set>() {
                        @Override
                        public Set call() throws Exception {
                            return (it.items.collect { it.type }) as Set;
                        }
                    })
                    .onError(Functions.constant(false))
                    )
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_UP)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            try {
                                return (it.serviceState == "STARTED");
                            }
                            catch (Exception e) {
                                return false;
                            }
                        }
                    })
                    .onError(Functions.constant(false))
                    )
                .poll(new FunctionPollConfig<List,List>(SERVICE_HEALTH)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            return it.healthSummary;
                        }
                    })
                    .onError(Functions.constant(false))
                    )
                .poll(new FunctionPollConfig<List,List>(SERVICE_URL)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            return it.serviceUrl;
                        }
                    })
                    .onError(Functions.constant(false))
                    )
                .build();
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
