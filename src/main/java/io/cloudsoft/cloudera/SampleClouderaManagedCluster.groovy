package io.cloudsoft.cloudera;

import groovy.transform.InheritConstructors
import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNode
import io.cloudsoft.cloudera.brooklynnodes.StartupGroup
import io.cloudsoft.cloudera.brooklynnodes.WhirrClouderaManager
import io.cloudsoft.cloudera.builders.HBaseTemplate
import io.cloudsoft.cloudera.builders.HdfsTemplate
import io.cloudsoft.cloudera.builders.MapReduceTemplate
import io.cloudsoft.cloudera.builders.ZookeeperTemplate
import io.cloudsoft.cloudera.rest.ClouderaRestCaller
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.Entity
import brooklyn.entity.basic.AbstractApplication
import brooklyn.entity.basic.Entities
import brooklyn.entity.group.DynamicCluster
import brooklyn.launcher.BrooklynLauncher
import brooklyn.location.Location
import brooklyn.location.basic.LocationRegistry
import brooklyn.util.CommandLineUtil

@InheritConstructors
public class SampleClouderaManagedCluster extends AbstractApplication {

    static final Logger log = LoggerFactory.getLogger(SampleClouderaManagedCluster.class);
    static final String DEFAULT_LOCATION = "aws-ec2:us-east-1";
    
    // Admin - Cloudera Manager Node
    Entity admin = new StartupGroup(this, name: "Cloudera Hosts and Admin");
    WhirrClouderaManager whirrCM = new WhirrClouderaManager(admin);
    
    // and CDH Hosts ("workers")
    DynamicCluster workerCluster = new DynamicCluster(admin, name: "CDH Nodes", 
        initialSize: 4, 
        factory: ClouderaCdhNode.newFactory().setConfig(ClouderaCdhNode.MANAGER, whirrCM));

    // Separate high-level Services bucket, populated below
    Entity services = new StartupGroup(this, name: "Cloudera Services");
    
    public void postStart(Collection<? extends Location> locations) {
        log.info("Application nodes started, now creating services");
        
        // create these in sequence
        // following builds with sensible defaults, showing a few different syntaxes
        
        new HdfsTemplate().
                manager(whirrCM).discoverHostsFromManager().
                assignRole(HdfsRoleType.NAMENODE).toAnyHost().
                assignRole(HdfsRoleType.SECONDARYNAMENODE).toAnyHost().
                assignRole(HdfsRoleType.DATANODE).toAllHosts().
                formatNameNodes().
                enableMetrics().
                buildWithEntity(services, manager: whirrCM);
            
        new MapReduceTemplate().
                named("mapreduce-sample").
                manager(whirrCM).discoverHostsFromManager().
                assignRoleJobTracker().toAnyHost().
                assignRoleTaskTracker().toAllHosts().
                enableMetrics().
                buildWithEntity(services);

        new ZookeeperTemplate().
                manager(whirrCM).discoverHostsFromManager().
                assignRoleServer().toAnyHost().
                buildWithEntity(services);

        new HBaseTemplate().
                manager(whirrCM).discoverHostsFromManager().
                assignRoleMaster().toAnyHost().
                assignRoleRegionServer().toAllHosts().
                buildWithEntity(services);
    }
    

    // can be started in usual Java way, or (bypassing method below)
    // with brooklyn command-line
        
    public static void main(String[] argv) {
        ArrayList args = new ArrayList(Arrays.asList(argv));
        int port = CommandLineUtil.getCommandLineOptionInt(args, "--port", 8081);
        List<Location> locations = new LocationRegistry().getLocationsById(args ?: [DEFAULT_LOCATION])

        SampleClouderaManagedCluster app = new SampleClouderaManagedCluster(name:'Brooklyn Cloudera Managed Cluster');
            
        try {
            BrooklynLauncher.manage(app, port)
            app.start(locations)
        } catch (Throwable t) {
            log.warn("FAILED TO START "+app+": "+t, t);
        }
        
        //dump some info
        Entities.dumpInfo(app)

        //open a console to interact
        Binding b = new Binding();
        b.setVariable("app", app);
        new groovy.ui.Console(b).run();
    }

}
