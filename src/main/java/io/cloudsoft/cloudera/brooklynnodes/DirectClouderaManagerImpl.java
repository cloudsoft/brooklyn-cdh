package io.cloudsoft.cloudera.brooklynnodes;

import static io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode.log;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiHostList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import io.cloudsoft.cloudera.rest.ClouderaApi;
import io.cloudsoft.cloudera.rest.ClouderaApiImpl;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.aws.util.AWSUtils;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.ec2.EC2ApiMetadata;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.domain.IpProtocol;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApiMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.render.RendererHints;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.collections.MutableSet;
import brooklyn.util.internal.Repeater;
import brooklyn.util.net.Cidr;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;

public class DirectClouderaManagerImpl extends SoftwareProcessImpl implements DirectClouderaManager {

    private static final Logger LOG = LoggerFactory.getLogger(DirectClouderaManagerImpl.class);
    private FunctionFeed functionFeed;

    static {
        RendererHints.register(CLOUDERA_MANAGER_URL, new RendererHints.NamedActionWithUrl("Open"));
    }

    @Override
    public Class getDriverInterface() {
        return DirectClouderaManagerDriver.class;
    }

    @Override
    protected Collection<Integer> getRequiredOpenPorts() {
        return MutableSet.<Integer>builder().addAll(super.getRequiredOpenPorts()).
                addAll(Arrays.asList(22, 2181, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030)).
                build();
    }

    private boolean isJcloudsLocation(MachineProvisioningLocation location, String providerName) {
        return location instanceof JcloudsLocation
                && ((JcloudsLocation) location).getProvider().equals(providerName);
    }
    
    @Override
    protected void postDriverStart() {
        super.postDriverStart();
        String cmHost;
        if(getConfig(ClouderaManagerNode.USE_IP_ADDRESS)) {
            cmHost = getAttribute(ADDRESS);
        } else {
            cmHost = getAttribute(HOSTNAME);
        }
        try {
            TempCloudUtils.authorizePing(new Cidr(), Iterables.getFirst(getLocations(), null));
        } catch (Throwable t) {
            log.warn("can't setup firewall/ping: "+t, t);
        }

        setAttribute(CLOUDERA_MANAGER_HOSTNAME, cmHost);
        setAttribute(CLOUDERA_MANAGER_URL, "http://" + cmHost + ":7180/");
        
        if (!Repeater.create(MutableMap.of("timeout", 5*60*1000, "description", "Waiting for successful REST call to "+this))
                    .rethrowException().repeat().every(1, TimeUnit.SECONDS)
                    .until(new Callable<Boolean>() {
                        public Boolean call() {
                            resetRootApi();
                            return (getRootApi().listClusters() != null);
                    }}).run()) {
            throw new IllegalStateException("Timeout waiting for successful REST call to ${this}");
        }
        log.debug("Detected REST online for DirectCM "+this);
        //TODO accept default license / configurable license ? (is just a gui thing though...)
        
        //TODO change password on server, ensure RestCallers are reset to use right password
    }

    private ClouderaApi _caller;
    public synchronized ClouderaApi getRootApi() {
        if (_caller != null) return _caller;
        // TODO use config
        return _caller = new ClouderaApiImpl(getAttribute(CLOUDERA_MANAGER_HOSTNAME), "admin", "admin");
    }
    private synchronized void resetRootApi() {
        _caller = null;
    }

    public ClouderaCdhNode findEntityForHostId(String hostId) {
        return findEntityForHostIdIn(hostId, getParent());
    }
    public static ClouderaCdhNode findEntityForHostIdIn(String hostId, Entity root) {
        if (LOG.isTraceEnabled())
            LOG.trace("hostId={}, CDH_NODE_ID={}, entity={}",
                    new Object[] {hostId, root.getAttribute(ClouderaCdhNode.CDH_HOST_ID), root});
        if ((root instanceof ClouderaCdhNode) && hostId.equals(root.getAttribute(ClouderaCdhNode.CDH_HOST_ID)))
            return (ClouderaCdhNode)root;
        for (Entity child: root.getChildren()) {
            ClouderaCdhNode result = findEntityForHostIdIn(hostId, child);
            if (result!=null) return result;
        }
        return null;
    }

    @Override
    public void connectSensors() {
        functionFeed = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_UP)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                try {
                                    return (getRootApi().listHosts()!=null);
                                } 
                                catch (Exception e) {
                                    log.error("Cannot execute getRestCaller().getHosts()", e); 
                                    return false; 
                                }
                            }
                          })
                          .onSuccess(Functions.<Boolean>identity())
                        )
                .poll(new FunctionPollConfig<List,List>(MANAGED_HOSTS)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<List>() {
                            @Override
                            public List call() throws Exception {
                                return getRootApi().listHosts();
                            }
                          })
                          .onSuccess(Functions.<List>identity())
                        )
                .poll(new FunctionPollConfig<List,List>(MANAGED_CLUSTERS)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<List>() {
                            @Override
                            public List call() throws Exception {
                                return getRootApi().listClusters();
                            }
                          })
                          .onSuccess(Functions.<List>identity())
                        )
                .build();
    }

    @Override
    protected void disconnectSensors() {
      super.disconnectSensors();
      if (functionFeed != null) functionFeed.stop();
    }    
    
    public static void authorizeIngress(ComputeServiceContext computeServiceContext,
        Set<Instance> instances, ClusterSpec clusterSpec, List<Cidr> cidrs, int... ports) {
        
        if (EC2ApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeServiceContext.getBackendType())) {
//        from:
//            FirewallManager.authorizeIngress(computeServiceContext, instances, clusterSpec, cidrs, ports);
            
            // This code (or something like it) may be added to jclouds (see
            // http://code.google.com/p/jclouds/issues/detail?id=336).
            // Until then we need this temporary workaround.
            String region = AWSUtils.parseHandle(Iterables.get(instances, 0).getId())[0];
            EC2Client ec2Client = computeServiceContext.unwrap(EC2ApiMetadata.CONTEXT_TOKEN).getApi();
            String groupName = "jclouds#" + clusterSpec.getClusterName();
            for (Cidr cidr : cidrs) {
                for (int port : ports) {
                    try {
                        ec2Client.getSecurityGroupServices()
                                .authorizeSecurityGroupIngressInRegion(region, groupName,
                                IpProtocol.TCP, port, port, cidr.toString());
                    } catch(IllegalStateException e) {
                        LOG.warn(e.getMessage());
                        /* ignore, it means that this permission was already granted */
                    }
                }
            }
        } else {
            // TODO generalise the above, or support more clouds, or bypass whirr
            LOG.debug("Skipping port ingress modifications for "+instances+" in cloud "+computeServiceContext.getBackendType());
        }
    }
        

}
