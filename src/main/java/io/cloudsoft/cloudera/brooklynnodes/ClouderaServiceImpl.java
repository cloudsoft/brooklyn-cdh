package io.cloudsoft.cloudera.brooklynnodes;

import brooklyn.util.collections.MutableMap;
import brooklyn.util.internal.Repeater;
import com.cloudera.api.model.ApiCommand;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceState;
import io.cloudsoft.cloudera.rest.ClouderaApi;
import io.cloudsoft.cloudera.rest.ClouderaApiImpl;

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

import com.cloudera.api.model.ApiRole;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ClouderaServiceImpl extends AbstractEntity implements ClouderaService {

   private static final Logger log = LoggerFactory.getLogger(ClouderaServiceImpl.class);
        
   private FunctionFeed feed;
   
   static {
       RendererHints.register(ClouderaService.SERVICE_URL, new RendererHints.NamedActionWithUrl("Open"));
   }

    public ClouderaApi getApi() {
        return new ClouderaApiImpl(getConfig(MANAGER).getAttribute(ClouderaManagerNode.CLOUDERA_MANAGER_HOSTNAME), "admin", "admin");
    }

    void startService() {
        final ApiCommand apiCommand = getApi().invokeServiceCommand(getClusterName(), getServiceName(), ClouderaApi.Command.START);
        retryIsCommandSuccessful(apiCommand);
    }

    void restartService() {
        final ApiCommand apiCommand = getApi().invokeServiceCommand(getClusterName(), getServiceName(), ClouderaApi.Command.RESTART);
        retryIsCommandSuccessful(apiCommand);
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
                .poll(new FunctionPollConfig<Boolean, Boolean>(SERVICE_REGISTERED)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                try {
                                    return (getApi().listServices(getClusterName()).contains(getServiceName()));
                                } catch (Exception e) {
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
                                List<ApiRole> roles = getApi().listServiceRoles(getClusterName(), getServiceName());

                                for (ApiRole role : roles) {
                                    ids.add(role.getHostRef().getHostId());
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
                                List<ApiRole> serviceRoles = getApi().listServiceRoles(getClusterName(), getServiceName());

                                for (ApiRole role : serviceRoles) {
                                    types.add(role.getType());
                                }

                                return types;
                            }
                        }))
                .poll(new FunctionPollConfig<Boolean, Boolean>(SERVICE_UP)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                try {
                                    ApiService service = getApi().getService(getClusterName(), getServiceName());

                                    return service.getServiceState().equals(ApiServiceState.STARTED);

                                    //return serviceJson.getString("serviceState").equals("STARTED");
                                } catch (Exception e) {
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
                            ApiService service = getApi().getService(getClusterName(), getServiceName());
                            return service.getHealthSummary().toString();
                            /*
                            JSONObject serviceJson = getApi().getServiceJson(getClusterName(), getServiceName());
                            return serviceJson.getString("healthSummary");
                            */
                        }
                    }))
                .poll(new FunctionPollConfig<String, String>(SERVICE_URL)
                    .period(30, TimeUnit.SECONDS)
                    .callable(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            ApiService service = getApi().getService(getClusterName(), getServiceName());
                            return service.getServiceUrl();
                            /*
                            JSONObject serviceJson = getApi().getServiceJson(getClusterName(), getServiceName());
                            return serviceJson.getString("serviceUrl");
                            */
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
        startService();
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
        restartService();
        setAttribute(SERVICE_STATE, Lifecycle.RUNNING);
    }


    private void retryIsCommandSuccessful(final ApiCommand apiCommand) {
        if (!Repeater.create(MutableMap.of("timeout", 5 * 60 * 1000, "description", "Waiting for successful REST call to " + this))
                .rethrowException().repeat().every(1, TimeUnit.SECONDS)
                .until(new Callable<Boolean>() {
                    public Boolean call() {
                        return getApi().isCommandSuccessful(apiCommand.getId());
                    }
                }).run()) {
            setAttribute(SERVICE_STATE, Lifecycle.ON_FIRE);
            throw new IllegalStateException("The service failed to " + ClouderaApi.Command.START + ".");
        }
    }

        /*
        boolean result = getApi().invokeServiceCommand(getClusterName(), getServiceName(), ClouderaApi.Command.START).block(5 * 60 * 1000);
        if (!result) {
            setAttribute(SERVICE_STATE, Lifecycle.ON_FIRE);
            throw new IllegalStateException("The service failed to " + cmd + ".");
        }
    }
        */
}
