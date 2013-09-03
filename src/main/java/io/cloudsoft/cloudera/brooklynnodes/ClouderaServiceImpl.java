package io.cloudsoft.cloudera.brooklynnodes;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.render.RendererHints;
import brooklyn.entity.basic.AbstractEntity;
import brooklyn.entity.basic.Description;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.NamedParameter;
import brooklyn.entity.trait.StartableMethods;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.location.Location;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ClouderaServiceImpl extends AbstractEntity implements ClouderaService {

   private static final Logger log = LoggerFactory.getLogger(ClouderaServiceImpl.class);
        
   private FunctionFeed feed;
   
   static {
       RendererHints.register(ClouderaService.SERVICE_URL, new RendererHints.NamedActionWithUrl("Open"));
   }

    public ClouderaRestCaller getApi() {
        return ClouderaRestCaller.newInstance(getConfig(MANAGER).getAttribute(ClouderaManagerNode.CLOUDERA_MANAGER_HOSTNAME), "admin", "admin");
    }
    
    void invokeServiceCommand(String cmd) {
        boolean result = getApi().invokeServiceCommand(getClusterName(), getServiceName(), cmd).block(5 * 60 * 1000);
        if (!result) {
            setAttribute(SERVICE_STATE, Lifecycle.ON_FIRE);
            throw new IllegalStateException("The service failed to " + cmd + ".");
        }
    }

    public void create() {
        if (getAttribute(SERVICE_STATE)!=null) {
            throw new IllegalStateException("${this} is already created");
        }
        setAttribute(SERVICE_STATE, Lifecycle.STARTING);
        
        log.info("Creating CDH service " + this.getDisplayName());
        Boolean buildResult = getConfig(TEMPLATE).build(getApi());
        log.info("Created CDH service " + this.getDisplayName() + ", result: " + buildResult);
        
        setAttribute(SERVICE_STATE, buildResult ? Lifecycle.RUNNING : Lifecycle.ON_FIRE);
        setAttribute(SERVICE_NAME, getConfig(TEMPLATE).getName());
        setAttribute(CLUSTER_NAME, getConfig(TEMPLATE).getClusterName());
        connectSensors();
    }
    
    public void disconnectSensors() {
       if (feed != null)
          feed.stop();
    }
    
    protected void connectSensors() {
        feed = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_REGISTERED)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            try {
                                return (getApi().getServices(getClusterName()).contains(getServiceName()));
                            }
                            catch (Exception e) {
                                return false;
                            }
                        }
                    })
                    .onException(Functions.constant(false)))
                .poll(new FunctionPollConfig<Collection<String>, Collection<String>>(HOSTS)
                        .period(30, TimeUnit.SECONDS).callable(new Callable<Collection<String>>() {
                            @Override
                            public Collection<String> call() throws Exception {
                                List<String> ids = Lists.newArrayList();
                                JSONObject serviceRolesJson = getApi().getServiceRolesJson(getClusterName(), getServiceName());
                                JSONArray array = serviceRolesJson.getJSONArray("items");
                                for (Object item : array) {
                                    String hostId = ((JSONObject) item).getJSONObject("hostRef").getString("hostId");
                                    ids.add(hostId);
                                }
                                return ids;
                            }
                        }))
                .poll(new FunctionPollConfig<Collection<String>, Collection<String>>(ROLES)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<Collection<String>>() {
                        @Override
                        public Collection<String> call() throws Exception {
                            Set<String> types = Sets.newHashSet();
                            JSONObject serviceRolesJson = getApi().getServiceRolesJson(getClusterName(), getServiceName());
                            JSONArray array = serviceRolesJson.getJSONArray("items");
                            for (Object item : array) {
                                String type = ((JSONObject) item).getString("type");
                                types.add(type);
                            }
                            return types;
                        }
                    }))
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_UP)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            try {
                                JSONObject serviceJson = getApi().getServiceJson(getClusterName(), getServiceName());
                                return serviceJson.getString("serviceState").equals("STARTED");
                            }
                            catch (Exception e) {
                                log.error("Can't connect to " + getServiceName(), e);
                                return false;
                            }
                        }
                    })
                    .onException(Functions.constant(false)))
                .poll(new FunctionPollConfig<String, String>(SERVICE_HEALTH)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            JSONObject serviceJson = getApi().getServiceJson(getClusterName(), getServiceName());
                            return serviceJson.getString("healthSummary");
                        }
                    }))
                .poll(new FunctionPollConfig<String, String>(SERVICE_URL)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            JSONObject serviceJson = getApi().getServiceJson(getClusterName(), getServiceName());
                            return serviceJson.getString("serviceUrl");
                        }
                    }))
                .build();
    }

    String getClusterName() { return getAttribute(CLUSTER_NAME); }
    String getServiceName() { return getAttribute(SERVICE_NAME); }

    @Override
    @Description("Start the process/service represented by an entity")
    public void start(@NamedParameter("locations") Collection<? extends Location> locations) {
        if (locations == null) log.debug("Ignoring locations at " + this);
        if (Lifecycle.RUNNING.equals(getAttribute(SERVICE_STATE))) {
            log.debug("Ignoring start when already started at " + this);
            return;
        }
        setAttribute(SERVICE_STATE, Lifecycle.STARTING);
        invokeServiceCommand("start");
        setAttribute(SERVICE_STATE, Lifecycle.RUNNING);
    }

    
    @Override
    @Description("Stop the process/service represented by an entity")
    public void stop() {
       StartableMethods.stop(this);
       disconnectSensors();
    }

    @Override
    @Description("Restart the process/service represented by an entity")
    public void restart() {
        setAttribute(SERVICE_STATE, Lifecycle.STARTING);
        invokeServiceCommand("restart");
        setAttribute(SERVICE_STATE, Lifecycle.RUNNING);
    }

}
