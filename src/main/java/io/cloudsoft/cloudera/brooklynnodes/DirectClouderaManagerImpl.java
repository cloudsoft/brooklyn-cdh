package io.cloudsoft.cloudera.brooklynnodes;

import static io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode.log;
import io.cloudsoft.cloudera.rest.ClouderaRestCaller;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.aws.util.AWSUtils;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.ec2.EC2ApiMetadata;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.domain.IpProtocol;
import org.jclouds.ec2.domain.Reservation;
import org.jclouds.ec2.domain.RunningInstance;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApiMetadata;

import brooklyn.config.render.RendererHints;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.location.Location;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import brooklyn.util.MutableMap;
import brooklyn.util.MutableSet;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.internal.Repeater;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class DirectClouderaManagerImpl extends SoftwareProcessImpl implements DirectClouderaManager {

    static {
        RendererHints.register(CLOUDERA_MANAGER_URL, new RendererHints.NamedActionWithUrl("Open"));
    }
    
	private FunctionFeed functionFeed;

//
//    @Override
//    public void init() {
//        super.init();
//        setConfig(APT_PROXY, getManagementContext().getConfig().getFirst(APT_PROXY.getName()));
//        setConfig(USE_IP_ADDRESS, getManagementContext().getConfig().getFirst(USE_IP_ADDRESS.getName()));
//    }
    
    @Override
    public Class getDriverInterface() {
        return DirectClouderaManagerDriver.class;
    }

    protected Collection<Integer> getRequiredOpenPorts() {
        return MutableSet.<Integer>builder().addAll(super.getRequiredOpenPorts()).
                addAll(Arrays.asList(22, 2181, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030)).
                build();
    }
    
    protected Map<String, Object> getProvisioningFlags(MachineProvisioningLocation location) {
        return obtainProvisioningFlags(location);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Map<String, Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        PortableTemplateBuilder portableTemplateBuilder = new PortableTemplateBuilder();
        if (isJcloudsLocation(location, "aws-ec2")) {
            portableTemplateBuilder.osFamily(OsFamily.UBUNTU).osVersionMatches("12.04").os64Bit(true).minRam(2500);
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(), portableTemplateBuilder);
        } else if (isJcloudsLocation(location, "google-compute-engine")) {
            flags.putAll(GoogleComputeEngineApiMetadata.defaultProperties());
            flags.put("groupId", "brooklyn-cdh");
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
                    portableTemplateBuilder.osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true)
                            .locationId("us-central1-a").minRam(2560));
        } else if (isJcloudsLocation(location, "openstack-nova")) {
            String imageId = location.getConfig(JcloudsLocationConfig.IMAGE_ID);
            if(imageId != null) {
                portableTemplateBuilder.imageId(imageId);
            }
            String hardwareId = location.getConfig(JcloudsLocationConfig.HARDWARE_ID);
            if(hardwareId != null) {
                portableTemplateBuilder.hardwareId(hardwareId);
            }
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(), portableTemplateBuilder);
        } else if (isJcloudsLocation(location, "rackspace-cloudservers-uk") || 
                isJcloudsLocation(location, "cloudservers-uk")) {
            // securityGroups are not supported
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
                    portableTemplateBuilder.osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true)
                    .minRam(2560));
        } else if (isJcloudsLocation(location, "bluelock-vcloud-zone01")) {
            System.setProperty("jclouds.vcloud.timeout.task-complete", 600 * 1000 + "");
            // this is a constraint for dns name on vcloud (3-15 characters)
            flags.put("groupId", "brooklyn");
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
            //    portableTemplateBuilder.osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true));
            portableTemplateBuilder.imageId("https://zone01.bluelock.com/api/v1.0/vAppTemplate/vappTemplate-e0717fc0-0b7f-41f7-a275-3e03881d99db"));
            }
        return flags;
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
            authorizePing("0.0.0.0/0", Iterables.getFirst(getLocations(), null));
        } catch (Throwable t) {
            log.warn("can't setup firewall/ping: "+t, t);
        }

        setAttribute(CLOUDERA_MANAGER_HOSTNAME, cmHost);
        setAttribute(CLOUDERA_MANAGER_URL, "http://" + cmHost + ":7180/");
        
        if (!Repeater.create(MutableMap.of("timeout", 5*60*1000, "description", "Waiting for successful REST call to "+this))
                    .rethrowException().repeat().every(1, TimeUnit.SECONDS)
                    .until(new Callable<Boolean>() {
                        public Boolean call() {
                            resetRestCaller();
                            return (getRestCaller().getClusters()!=null);
                    }}).run()) {
            throw new IllegalStateException("Timeout waiting for successful REST call to ${this}");
        }
        log.debug("Detected REST online for DirectCM "+this);
        //TODO accept default license / configurable license ? (is just a gui thing though...)
        
        //TODO change password on server, ensure RestCallers are reset to use right password
    }

    private ClouderaRestCaller _caller;
	
    public synchronized ClouderaRestCaller getRestCaller() {
        if (_caller != null) return _caller;
        // TODO use config
        return _caller = ClouderaRestCaller.newInstance(getAttribute(CLOUDERA_MANAGER_HOSTNAME),
            "admin", "admin");
    }
    private synchronized void resetRestCaller() {
        _caller = null;
    }

    public ClouderaCdhNode findEntityForHostId(String hostId) {
        return findEntityForHostIdIn(hostId, getParent());
    }
    public static ClouderaCdhNode findEntityForHostIdIn(String hostId, Entity root) {
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
        super.connectSensors();
        
        functionFeed = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_UP)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                try { 
                                    return (getRestCaller().getHosts()!=null); 
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
                                return getRestCaller().getHosts();
                            }
                          })
                          .onSuccess(Functions.<List>identity())
                        )
                .poll(new FunctionPollConfig<List,List>(MANAGED_CLUSTERS)
                        .period(30, TimeUnit.SECONDS)
                        .callable(new Callable<List>() {
                            @Override
                            public List call() throws Exception {
                                return getRestCaller().getClusters();
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
        Set<Instance> instances, ClusterSpec clusterSpec, List<String> cidrs, int... ports) {
        
        if (EC2ApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeServiceContext.getBackendType())) {
//        from:
//            FirewallManager.authorizeIngress(computeServiceContext, instances, clusterSpec, cidrs, ports);
            
            // This code (or something like it) may be added to jclouds (see
            // http://code.google.com/p/jclouds/issues/detail?id=336).
            // Until then we need this temporary workaround.
            String region = AWSUtils.parseHandle(Iterables.get(instances, 0).getId())[0];
            EC2Client ec2Client = computeServiceContext.unwrap(EC2ApiMetadata.CONTEXT_TOKEN).getApi();
            String groupName = "jclouds#" + clusterSpec.getClusterName();
            for (String cidr : cidrs) {
                for (int port : ports) {
                    try {
                        ec2Client.getSecurityGroupServices()
                                .authorizeSecurityGroupIngressInRegion(region, groupName,
                                IpProtocol.TCP, port, port, cidr);
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
        
    private void authorizePing(String cidr, Location ssh) {
        JcloudsLocation jcl = null;
        JcloudsSshMachineLocation jclssh = null;
        if (ssh instanceof JcloudsSshMachineLocation) {
            jclssh = ((JcloudsSshMachineLocation)ssh);
            jcl = jclssh.getParent();
        }
        if (jcl!=null) {
            ComputeServiceContext ctx = jcl.getComputeService().getContext();
            if (ctx.unwrap() instanceof EC2Client) {
                // This code (or something like it) may be added to jclouds (see
                // http://code.google.com/p/jclouds/issues/detail?id=336).
                // Until then we need this temporary workaround.
//                String region = AWSUtils.parseHandle(Iterables.get(instances, 0).getId())[0];
                try {
                    String region = jcl.getRegion();
                    EC2Client ec2Client = EC2Client.class.cast(ctx.unwrap());
                    String id = jclssh.getNode().getId();
                    // string region prefix from id
                    if (id.indexOf('/')>=0) id = id.substring(id.indexOf('/')+1);
                    Set<? extends Reservation<? extends RunningInstance>> instances = ec2Client.getInstanceServices().describeInstancesInRegion(region, id);
                    Set<String> groupNames = Iterables.getOnlyElement(instances).getGroupNames(); 
                    String groupName = Iterables.getFirst(groupNames, null);
                    //                  "jclouds#" + clusterSpec.getClusterName(); // + "#" + region;
                    log.info("Authorizing ping for "+groupName+": "+cidr);
                    ec2Client.getSecurityGroupServices()
                        .authorizeSecurityGroupIngressInRegion(region, groupName,
                            IpProtocol.ICMP, -1, -1, cidr);
                    return;
                } catch(Exception e) {
                    log.warn("Problem authorizing ping (possibly already authorized) for "+this+": "+e.getMessage());
                    log.debug("Details for roblem authorizing ping (possibly already authorized) for "+this+": "+e.getMessage(), e);
                    /* ignore, usually means that this permission was already granted */
                    Exceptions.propagateIfFatal(e);
                }
            }
        }
        LOG.debug("Skipping ping authorization for "+this+"; not required or not supported");
        // TODO for JcloudsSsh and Jclouds -- in AWS
        // see WhirrClusterManager for example
    }


}
