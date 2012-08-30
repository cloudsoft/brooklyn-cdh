package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import io.cloudsoft.cloudera.rest.RestDataObjects.MapReduceRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceRoleHostInfo;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.util.IdGenerator;

public class MapReduceTemplate extends ServiceTemplate<MapReduceTemplate> {

    private static final Logger log = LoggerFactory.getLogger(MapReduceTemplate.class);
    
    public RoleAssigner<MapReduceTemplate> assignRole(MapReduceRoleType role) {
        return assignRole(role.name());
    }

    protected boolean abortIfServiceExists = false;
    public MapReduceTemplate abortIfServiceExists() {
        abortIfServiceExists = true;
        return this;
    }

    public RoleAssigner<MapReduceTemplate> assignRoleJobTracker() {
        return assignRole(MapReduceRoleType.JOBTRACKER);
    }
    public RoleAssigner<MapReduceTemplate> assignRoleTaskTracker() {
        return assignRole(MapReduceRoleType.TASKTRACKER);
    }
    public RoleAssigner<MapReduceTemplate> assignRoleGateway() {
        return assignRole(MapReduceRoleType.GATEWAY);
    }

    @Override
    public Object build(ClouderaRestCaller caller) {
        if (name==null) name = "mapreduce-"+IdGenerator.makeRandomId(8);
        
        List<String> clusters = caller.getClusters();
        if (clusterName==null) {
            if (!clusters.isEmpty()) clusterName = clusters.iterator().next();
            else clusterName = "cluster-"+IdGenerator.makeRandomId(6);
        }
        if (!clusters.contains(clusterName)) caller.addCluster(clusterName);

        List<String> hdfss = caller.findServicesOfType(clusterName, ServiceType.HDFS);
        if (hdfss.isEmpty()) throw new IllegalStateException("HDFS cluster required before can start MAPREDUCE");
        
        List<String> services = caller.getServices(clusterName);
        if (abortIfServiceExists && services.contains(name))
            return true;
        
        caller.addService(clusterName, name, ServiceType.MAPREDUCE);
        caller.addServiceRoleHosts(clusterName, name, roles.toArray(new ServiceRoleHostInfo[0]));
        
        Object config = caller.getServiceConfig(clusterName, name);
        RestDataObjects.setConfig(config, "hdfs_service", hdfss.iterator().next());
        Map<?,?> cfgOut = RestDataObjects.convertConfigForSetting(config, clusterName+"-"+name);
        log.debug("MapR converted config: "+cfgOut);
        caller.setServiceConfig(clusterName, name, cfgOut);

        return caller.invokeServiceCommand(clusterName, name, "start").block(60*1000);
    }

    
    public static void main(String[] args) {
        String SERVER = "ec2-107-22-7-107.compute-1.amazonaws.com",
            H1 = "ip-10-114-39-149.ec2.internal",
            H2 = "ip-10-144-18-90.ec2.internal",
            H3 = "ip-10-60-154-21.ec2.internal";
        
        ClouderaRestCaller caller = ClouderaRestCaller.newInstance(SERVER, "admin", "admin");

        System.out.println(new MapReduceTemplate().
                hosts(H1, H2, H3).
                assignRole(MapReduceRoleType.JOBTRACKER).to(H1).
                assignRole(MapReduceRoleType.TASKTRACKER).toAllHosts().
            build(caller));
    }


}
