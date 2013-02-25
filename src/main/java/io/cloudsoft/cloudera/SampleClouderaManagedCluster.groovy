package io.cloudsoft.cloudera;

import io.cloudsoft.cloudera.brooklynnodes.AllServices
import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNode
import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNodeImpl
import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode
import io.cloudsoft.cloudera.brooklynnodes.ClouderaService
import io.cloudsoft.cloudera.brooklynnodes.DirectClouderaManager
import io.cloudsoft.cloudera.brooklynnodes.StartupGroup
import io.cloudsoft.cloudera.brooklynnodes.WhirrClouderaManager
import io.cloudsoft.cloudera.builders.HBaseTemplate
import io.cloudsoft.cloudera.builders.HdfsTemplate
import io.cloudsoft.cloudera.builders.MapReduceTemplate
import io.cloudsoft.cloudera.builders.ZookeeperTemplate
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.catalog.Catalog
import brooklyn.enricher.basic.SensorPropagatingEnricher
import brooklyn.entity.Entity
import brooklyn.entity.basic.AbstractApplication
import brooklyn.entity.basic.ApplicationBuilder
import brooklyn.entity.basic.Entities
import brooklyn.entity.group.DynamicCluster
import brooklyn.entity.proxying.BasicEntitySpec
import brooklyn.event.AttributeSensor
import brooklyn.launcher.BrooklynLauncher
import brooklyn.location.Location
import brooklyn.util.CommandLineUtil

@Catalog(name="Cloudera CDH4", 
    description="Launches Cloudera Distribution for Hadoop Manager with a Cloudera Manager and an initial cluster of 4 CDH nodes (resizable) and default services including HDFS, MapReduce, and HBase",
    iconUrl="classpath://io/cloudsoft/cloudera/cloudera.jpg")
public class SampleClouderaManagedCluster extends AbstractApplication implements SampleClouderaManagedClusterInterface {

    static final Logger log = LoggerFactory.getLogger(SampleClouderaManagedCluster.class);
    static final String DEFAULT_LOCATION = "aws-ec2:us-east-1";
        
    public SampleClouderaManagedCluster() {
        super();
    }
    public SampleClouderaManagedCluster(Map properties, Entity parent) {
        super(properties, parent);
    }
    public SampleClouderaManagedCluster(Map properties) {
        super(properties);
    }

    // Admin - Cloudera Manager Node
    protected Entity admin;
    protected ClouderaManagerNode clouderaManagerNode;
    protected DynamicCluster workerCluster;
    protected AllServices services;
    
    public StartupGroup getAdmin() { return admin; }
    public ClouderaManagerNode getManager() { return clouderaManagerNode; }
    public AllServices getServices() { return services; }
    
    public void postConstruct() {
        admin = addChild(getEntityManager().createEntity(BasicEntitySpec.newInstance(StartupGroup.class).
            displayName("Cloudera Hosts and Admin")) );
        clouderaManagerNode = admin.addChild(getEntityManager().createEntity(BasicEntitySpec.newInstance(
//            WhirrClouderaManager.class
            DirectClouderaManager.class
            )) );
        workerCluster = admin.addChild(getEntityManager().createEntity(BasicEntitySpec.newInstance(DynamicCluster.class).
            displayName("CDH Nodes").
            configure("factory", ClouderaCdhNodeImpl.newFactory().setConfig(ClouderaCdhNode.MANAGER, clouderaManagerNode)).
            configure("initialSize", 4)
            ) );
        services = addChild(getEntityManager().createEntity(BasicEntitySpec.newInstance(AllServices.class).
            displayName("Cloudera Services") ));
        addEnricher(SensorPropagatingEnricher.newInstanceListeningTo(clouderaManagerNode, ClouderaManagerNode.CLOUDERA_MANAGER_URL));
    }

    boolean launchDefaultServices = true;
    public void launchDefaultServices(boolean enabled) { launchDefaultServices = enabled; }    

    public void postStart(Collection<? extends Location> locations) {
        if (launchDefaultServices) {
            log.info("Application nodes started, now creating services");
            startServices(true, true);
        }
    }

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
                buildWithEntity(services, manager: clouderaManagerNode);
            
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
        if (hb) {
            log.info("Restarting HBase after Zookeeper restart");
            hb.restart();
        }
        log.info("CDH services now online -- "+clouderaManagerNode.getAttribute(ClouderaManagerNode.CLOUDERA_MANAGER_URL));
    }
    
    // can be started in usual Java way, or (bypassing method below)
    // with brooklyn command-line
        
    public static void main(String[] argv) {
        ArrayList args = new ArrayList(Arrays.asList(argv));
        SampleClouderaManagedClusterInterface app;
            
        try {
            def server = BrooklynLauncher.newLauncher().
                webconsolePort(CommandLineUtil.getCommandLineOption(args, "--port", "8081+")).
                launch();
                
            List<Location> locations = server.getManagementContext().getLocationRegistry().resolve(args ?: [DEFAULT_LOCATION])
            
            BasicEntitySpec<SampleClouderaManagedClusterInterface,?> appSpec = BasicEntitySpec.newInstance(SampleClouderaManagedClusterInterface.class).
                displayName("Brooklyn Cloudera Managed Cluster");
    
            app = ApplicationBuilder.builder(appSpec).
                manage(server.getManagementContext());
            app.launchDefaultServices(false);
            app.start(locations);
            
        } catch (Throwable t) {
            log.warn("FAILED TO START "+app+": "+t, t);
        }
        
        // describe what we've build
        Entities.dumpInfo(app);

        // now manually start some services (but not hbase)
        app.startServices(true, false);
    }

}
