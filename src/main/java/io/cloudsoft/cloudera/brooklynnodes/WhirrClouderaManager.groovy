package io.cloudsoft.cloudera.brooklynnodes;

import java.util.List;
import java.util.Set;

import org.apache.whirr.ClusterSpec;
import org.apache.whirr.Cluster.Instance;
import org.jclouds.compute.ComputeServiceContext;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller

import java.util.concurrent.TimeUnit

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.whirr.Cluster
import org.apache.whirr.ClusterSpec
import org.apache.whirr.RolePredicates
import org.apache.whirr.Cluster.Instance
import org.apache.whirr.service.FirewallManager
import org.jclouds.aws.util.AWSUtils
import org.jclouds.cloudservers.CloudServersApiMetadata;
import org.jclouds.compute.ComputeServiceContext
import org.jclouds.ec2.EC2ApiMetadata;
import org.jclouds.ec2.EC2Client
import org.jclouds.ec2.domain.IpProtocol
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.config.render.RendererHints
import brooklyn.entity.Entity
import brooklyn.event.adapter.FunctionSensorAdapter;
import brooklyn.event.adapter.SensorRegistry;
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.extras.whirr.core.WhirrCluster
import brooklyn.location.Location
import brooklyn.location.basic.jclouds.JcloudsLocation
import brooklyn.util.GroovyJavaMethods;
import brooklyn.util.NetworkUtils;
import brooklyn.util.flags.SetFromFlag
import brooklyn.util.internal.Repeater

import com.google.common.base.Joiner
import com.google.common.base.Preconditions
import com.google.common.collect.Iterables
import com.google.common.collect.Lists


public class WhirrClouderaManager extends WhirrCluster {

    public static final Logger log = LoggerFactory.getLogger(WhirrClouderaManager.class);

    @SetFromFlag("name")
    public static final BasicConfigKey<String> NAME =
        [String, "whirr.cm.name", "The name of the CM cluster"]

    @SetFromFlag("memory")
    public static final BasicConfigKey<Integer> MEMORY =
        [Integer, "whirr.cm.memory", "The minimum amount of memory to use for the CM node (in megabytes)", 2560]

    public static final BasicAttributeSensor<String> CLOUDERA_MANAGER_HOSTNAME =
        [String, "whirr.cm.hostname", "Public hostname for the Cloudera Manager node"]

    public static final BasicAttributeSensor<String> CLOUDERA_MANAGER_URL =
        [String, "whirr.cm.url", "URL for the Cloudera Manager node"]
    static {
        RendererHints.register(CLOUDERA_MANAGER_URL, new RendererHints.NamedActionWithUrl("Open"));
    }

    public static final BasicAttributeSensor<List> MANAGED_HOSTS =
        [List, "whirr.cm.hosts", "List of hosts managed by this CM instance"]

    public static final BasicAttributeSensor<List> MANAGED_CLUSTERS =
        [List, "whirr.cm.clusters", "List of clusters managed by this CM instance"]


    public WhirrClouderaManager(Entity owner) {
        this([:], owner);
    }
    public WhirrClouderaManager(Map flags = [:], Entity owner = null) {
        super(flags, owner)
        generateWhirrClusterRecipe();
    }

    public void generateWhirrClusterRecipe() {
        Preconditions.checkArgument(getConfig(MEMORY) >= 1000, "We need at least 1GB of memory per machine")

        List<String> recipeLines = Lists.newArrayList(
                "whirr.cluster-name=" + (((String)getConfig(NAME))?:"brooklyn-"+System.getProperty("user.name")+"-whirr-cloudera-manager").replaceAll("\\s+","-"),
                "whirr.instance-templates=1 cmserver"
        );

        if (userRecipeLines) recipeLines.addAll(userRecipeLines);
        setConfig(RECIPE, Joiner.on("\n").join(recipeLines));
    }
    
    List userRecipeLines = [];
    public void addRecipeLine(String line) {
        userRecipeLines << line;
        String r = getConfig(RECIPE) ?: "";
        if (r) r += "\n";
        r += line;
        setConfig(RECIPE, r);
    }

    private ClouderaRestCaller _caller;
    public synchronized ClouderaRestCaller getRestCaller() {
        if (_caller != null) return _caller;
        return _caller = new ClouderaRestCaller(server: getAttribute(CLOUDERA_MANAGER_HOSTNAME),
            authName: "admin", authPass: "admin");
    }
    private synchronized void resetRestCaller() {
        _caller = null;
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        super.start(locations);
        // TODO better way to override parent setting SERVICE_UP to true
        setAttribute(SERVICE_UP, false);
        
        Cluster.Instance cmServer = cluster.getInstanceMatching(RolePredicates.role("cmserver"));
        String cmHost = cmServer.publicHostName
        // the above can come back being the internal hostname, in some situations
        try {
            InetAddress addr = NetworkUtils.resolve(cmHost);
            if (addr==null) throw new NullPointerException("Cannot resolve "+cmHost);
            log.debug("Whirr-reported hostname "+cmHost+" for "+this+" resolved as "+addr);
        } catch (Exception e) {
            if (GroovyJavaMethods.truth(cmServer.publicIp)) {
                log.info("Whirr-reported hostname "+cmHost+" for "+this+" is not resolvable. Reverting to public IP "+cmServer.publicIp);
                cmHost = cmServer.publicIp;
            } else {
                log.warn("Whirr-reported hostname "+cmHost+" for "+this+" is not resolvable. No public IP available. Service may be unreachable.");
            }
        }

        try {
            authorizeIngress(controller.getCompute().apply(clusterSpec), [cmServer] as Set, clusterSpec,
                ["0.0.0.0/0"], 22, 2181, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030);
            authorizePing(controller.getCompute().apply(clusterSpec), [cmServer], clusterSpec);
        } catch (Throwable t) {
            log.warn("can't setup firewall/ping: "+t);
        }

        setAttribute(CLOUDERA_MANAGER_HOSTNAME, cmHost)
        setAttribute(CLOUDERA_MANAGER_URL, "http://" + cmHost + ":7180/")
        
        if (!Repeater.create(timeout:5*60*1000, description:"Waiting for successful REST call to ${this}")
                    .rethrowException().repeat().every(1*TimeUnit.SECONDS)
                    .until() {
                        resetRestCaller();
                        return (getRestCaller().getClusters()!=null);
                    }
                    .run()) {
            throw new IllegalStateException("Timeout waiting for successful REST call to ${this}");
        }
        log.debug("Detected REST online for WhirrCM ${this}")
        connectSensors();
        //TODO accept default license / configurable license ? (is just a gui thing though...)
        
        //TODO change password on server, ensure RestCallers are reset to use right password
        
//        setAttribute(SERVICE_UP, true);
    }
    
    protected transient SensorRegistry sensorRegistry;
    
    public void connectSensors() {
        if (!sensorRegistry) sensorRegistry = new SensorRegistry(this)
//        ConfigSensorAdapter.apply(this);

        FunctionSensorAdapter fnSensorAdaptor = sensorRegistry.register(new FunctionSensorAdapter({}, period: 30*TimeUnit.SECONDS));
        fnSensorAdaptor.poll(SERVICE_UP, { try { return (getRestCaller().getHosts()!=null) } catch (Exception e) { return false; } });
        fnSensorAdaptor.poll(MANAGED_HOSTS, { getRestCaller().getHosts() });
        fnSensorAdaptor.poll(MANAGED_CLUSTERS, { getRestCaller().getClusters() });
        
        sensorRegistry.activateAdapters();
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
        
    public static void authorizePing(ComputeServiceContext computeServiceContext,
            Collection<Instance> instances, ClusterSpec clusterSpec) {
        authorizePing(computeServiceContext, instances, clusterSpec, "0.0.0.0/0");
    }
    public static void authorizePing(ComputeServiceContext computeServiceContext,
            Collection<Instance> instances, ClusterSpec clusterSpec, String cidr) {
        if (computeServiceContext.getProviderSpecificContext().getApi() instanceof EC2Client) {
            // This code (or something like it) may be added to jclouds (see
            // http://code.google.com/p/jclouds/issues/detail?id=336).
            // Until then we need this temporary workaround.
            String region = AWSUtils.parseHandle(Iterables.get(instances, 0).getId())[0];
            EC2Client ec2Client = EC2Client.class.cast(
                    computeServiceContext.getProviderSpecificContext().getApi());
            String groupName = "jclouds#" + clusterSpec.getClusterName(); // + "#" + region;
            log.info("Authorizing ping for "+groupName+": "+cidr);
            try {
                ec2Client.getSecurityGroupServices()
                        .authorizeSecurityGroupIngressInRegion(region, groupName,
                        IpProtocol.ICMP, -1, -1, cidr);
            } catch(IllegalStateException e) {
                log.warn("Problem authorizing ping for "+groupName+": "+e.getMessage());
                /* ignore, it means that this permission was already granted */
            }
        } else {
            // TODO generalise the above, or support more clouds, or bypass whirr
            LOG.debug("Skipping ping ingress modifications for "+instances+" in cloud "+computeServiceContext.getBackendType());
        }
    }
    protected void customizeClusterSpecConfiguration(JcloudsLocation location, PropertiesConfiguration config) {
        super.customizeClusterSpecConfiguration(location, config);
        String templateSpec = "os64Bit=true,"+
                "minRam="+getConfig(MEMORY)+
                ",osFamily=CENTOS";   // Ubuntu machines on amazon don't seem to apt-get update correctly!
        if (location.getConf().providerLocationId)
            templateSpec += ",locationId="+location.getConf().providerLocationId;
        config.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), templateSpec);
    }

    @Override
    public void stop() {
        super.stop()
    }
    
    public ClouderaCdhNode findEntityForHostId(String hostId) {
        return findEntityForHostIdIn(hostId, owner);
    }
    public static ClouderaCdhNode findEntityForHostIdIn(String hostId, Entity root) {
        if ((root in ClouderaCdhNode) && hostId.equals(root.getAttribute(ClouderaCdhNode.CDH_HOST_ID)))
            return root;
        for (Entity child: root.ownedChildren) {
            ClouderaCdhNode result = findEntityForHostIdIn(hostId, child);
            if (result) return result;
        }
        return null;
    }

}
