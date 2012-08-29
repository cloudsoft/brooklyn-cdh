package io.cloudsoft.cloudera;

import java.util.Collection;

import groovy.transform.InheritConstructors
import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNode
import io.cloudsoft.cloudera.brooklynnodes.StartupGroup
import io.cloudsoft.cloudera.brooklynnodes.WhirrClouderaManager
import io.cloudsoft.cloudera.builders.HdfsTemplate;
import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType;

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
    
    Entity admin = new StartupGroup(this, name: "Cloudera Hosts and Admin");
    WhirrClouderaManager whirrCM = new WhirrClouderaManager(admin);
    DynamicCluster workerCluster = new DynamicCluster(admin, name: "CDH Nodes", initialSize: 3, 
        factory: ClouderaCdhNode.newFactory().setConfig(ClouderaCdhNode.MANAGER, whirrCM));

    Entity services = new StartupGroup(this, name: "Cloudera Services");
    
    ClouderaRestCaller api;
    
    public void postStart(Collection<? extends Location> locations) {
        api = new ClouderaRestCaller(server: whirrCM.getAttribute(WhirrClouderaManager.CLOUDERA_MANAGER_HOSTNAME), authName:"admin", authPass: "admin");
        
        log.info("Application nodes started, now creating HDFS");
        
        Object hdfsService = new HdfsTemplate().
                named("hdfs-1").
                hosts(whirrCM.getAttribute(WhirrClouderaManager.MANAGED_HOSTS)).
                assignRole(HdfsRoleType.NAMENODE).toAnyHost().
                assignRole(HdfsRoleType.SECONDARYNAMENODE).toAnyHost().
                assignRole(HdfsRoleType.DATANODE).toAllHosts().
                formatNameNodes().
            build(api);
    }
    
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
