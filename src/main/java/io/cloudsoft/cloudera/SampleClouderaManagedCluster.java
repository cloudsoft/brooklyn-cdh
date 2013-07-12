package io.cloudsoft.cloudera;

import io.cloudsoft.cloudera.brooklynnodes.AllServices;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNode;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNodeImpl;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaService;
import io.cloudsoft.cloudera.brooklynnodes.DirectClouderaManager;
import io.cloudsoft.cloudera.brooklynnodes.StartupGroup;
import io.cloudsoft.cloudera.builders.HBaseTemplate;
import io.cloudsoft.cloudera.builders.HdfsTemplate;
import io.cloudsoft.cloudera.builders.MapReduceTemplate;
import io.cloudsoft.cloudera.builders.ZookeeperTemplate;
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.catalog.Catalog;
import brooklyn.enricher.basic.SensorPropagatingEnricher;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.network.bind.BindDnsServer;
import brooklyn.entity.proxying.BasicEntitySpec;
import brooklyn.entity.proxying.EntitySpecs;
import brooklyn.entity.trait.Configurable;
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.location.Location;
import brooklyn.location.vmware.vcloud.director.VCloudDirectorLocation;
import brooklyn.util.CommandLineUtil;

import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

@Catalog(name = "Cloudera CDH4", description = "Launches Cloudera Distribution for Hadoop Manager with a Cloudera Manager and an initial cluster of 4 CDH nodes (resizable) and default services including HDFS, MapReduce, and HBase", iconUrl = "classpath://io/cloudsoft/cloudera/cloudera.jpg")
public class SampleClouderaManagedCluster extends AbstractApplication implements SampleClouderaManagedClusterInterface {

    static final Logger log = LoggerFactory.getLogger(SampleClouderaManagedCluster.class);
    static final String DEFAULT_LOCATION = "aws-ec2:us-east-1";

    // Admin - Cloudera Manager Node
    protected Entity admin;
    protected ClouderaManagerNode clouderaManagerNode;
    protected DynamicCluster workerCluster;
    protected AllServices services;
    protected BindDnsServer dnsServer;

    boolean launchDefaultServices = true;

    public StartupGroup getAdmin() {
        return (StartupGroup) admin;
    }

    public ClouderaManagerNode getManager() {
        return clouderaManagerNode;
    }

    public AllServices getServices() {
        return services;
    }
    
    @Override
    public void init() {
    	if (getConfig(SETUP_DNS)) {
    	    dnsServer = addChild(EntitySpecs.spec(BindDnsServer.class).displayName("dns-server")
    	            .configure("filter", Predicates.or(Predicates.instanceOf(ClouderaManagerNode.class), Predicates.instanceOf(ClouderaCdhNode.class)))
    	            .configure("domainName", "cloudera")
    	            .configure(BindDnsServer.REPLACE_RESOLV_CONF, true)
    	            .configure("hostnameSensor", ClouderaManagerNode.LOCAL_HOSTNAME));
    	}
    	
        admin = addChild(EntitySpecs.spec(StartupGroup.class).displayName("Cloudera Hosts and Admin"));

        clouderaManagerNode = (ClouderaManagerNode) admin.addChild(getEntityManager().createEntity(
                EntitySpecs.spec(DirectClouderaManager.class)));

        workerCluster = (DynamicCluster) admin.addChild(getEntityManager().createEntity(
                EntitySpecs
                        .spec(DynamicCluster.class)
                        .displayName("CDH Nodes")
                        .configure(
                                "factory",
                                ClouderaCdhNodeImpl.newFactory()
                                        .setConfig(ClouderaCdhNode.MANAGER, clouderaManagerNode))
                        .configure("initialSize", getConfig(CLUSTER_SIZE))));

        services = (AllServices) addChild(getEntityManager().createEntity(
                BasicEntitySpec.newInstance(AllServices.class).displayName("Cloudera Services")));
        
        addEnricher(SensorPropagatingEnricher.newInstanceListeningTo(clouderaManagerNode,
                ClouderaManagerNode.CLOUDERA_MANAGER_URL));
    }

    @Override
    public void start(Collection<? extends Location> locations) {
    	for (Location loc : locations) {
    		if (loc instanceof VCloudDirectorLocation) {
    			Integer cpuCount = getConfig(CPU_COUNT);
    			Long memorySize = getConfig(MEMORY_SIZE_MB);
    			Long secondaryDiskSizeGb = getConfig(SECOND_DISK_SIZE_GB);
    			if (cpuCount != null) ((Configurable)loc).setConfig(VCloudDirectorLocation.CPU_COUNT, cpuCount);
				if (memorySize != null) ((Configurable)loc).setConfig(VCloudDirectorLocation.MEMORY_SIZE_MB, memorySize);
                if (secondaryDiskSizeGb != null && secondaryDiskSizeGb > 0) {
                    ((Configurable)loc).setConfig(VCloudDirectorLocation.SECOND_DISK_SIZE_MB, secondaryDiskSizeGb*1000);
                    ((Configurable)loc).setConfig(VCloudDirectorLocation.MOUNT_POINT, "/mnt/data");
                } else {
                    ((Configurable)loc).setConfig(VCloudDirectorLocation.SECOND_DISK_SIZE_MB, null);
                }
    		}
    	}
    	
        Stopwatch stopwatch = new Stopwatch().start();

    	super.start(locations);
    	
        Entities.dumpInfo(this);
        
        log.info("Starting CDH services for {} (startup time so far is {} seconds)", this, stopwatch.elapsedTime(TimeUnit.SECONDS));
        startServices(true, false);
        
        stopwatch.stop(); 
        log.info("Time to deploy " + locations + ": " + stopwatch.elapsedTime(TimeUnit.SECONDS) + " seconds");
    }
	
    @Override
    public void launchDefaultServices(boolean enabled) {
        launchDefaultServices = enabled;
    }

    @Override
    public void startServices(boolean isCertificationCluster, boolean includeHbase) {
        // create these in sequence
        // following builds with sensible defaults, showing a few different syntaxes
        
        new HdfsTemplate().
                manager(clouderaManagerNode).discoverHostsFromManager().
                assignRole(HdfsRoleType.NAMENODE).toAnyHost().
                assignRole(HdfsRoleType.SECONDARYNAMENODE).toAnyHost().
                assignRole(HdfsRoleType.DATANODE).toAllHosts().
                formatNameNodes().
                enableMetrics(isCertificationCluster).
                buildWithEntity(clouderaManagerNode);
            
        new MapReduceTemplate().
                named("mapreduce-sample").
                manager(clouderaManagerNode).discoverHostsFromManager().
                assignRoleJobTracker().toAnyHost().
                assignRoleTaskTracker().toAllHosts().
                enableMetrics(isCertificationCluster).
                buildWithEntity(services);

        ClouderaService zk = new ZookeeperTemplate().
                manager(clouderaManagerNode).discoverHostsFromManager().
                assignRoleServer().toAnyHost().
                buildWithEntity(services);

        ClouderaService hb = null;
        if (includeHbase) {
            hb = new HBaseTemplate().
                manager(clouderaManagerNode).discoverHostsFromManager().
                assignRoleMaster().toAnyHost().
                assignRoleRegionServer().toAllHosts().
                buildWithEntity(services);
        }
                
        // seems to want a restart of ZK then HB after configuring HB
        log.info("Restarting Zookeeper after configuration change");
        zk.restart();
        if (hb != null) {
            log.info("Restarting HBase after Zookeeper restart");
            hb.restart();
        }
        log.info("CDH services now online -- "+clouderaManagerNode.getAttribute(ClouderaManagerNode.CLOUDERA_MANAGER_URL));
    }

    /**
     * Launches the application, along with the brooklyn web-console.
     */
    public static void main(String[] argv) throws Exception {
        List<String> args = Lists.newArrayList(argv);
        String port = CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", DEFAULT_LOCATION);

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                                                    .application(
                                                            EntitySpecs.appSpec(SampleClouderaManagedClusterInterface.class)
                                                            .displayName("Brooklyn Cloudera Managed Cluster"))
                                                    .webconsolePort(port)
                                                    .location(location)
                                                    .start();
    }
}
