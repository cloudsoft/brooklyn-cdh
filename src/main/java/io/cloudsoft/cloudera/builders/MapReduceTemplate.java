package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNode;
import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import io.cloudsoft.cloudera.rest.RestDataObjects.MapReduceRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceRoleHostInfo;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceTemplate extends ServiceTemplate<MapReduceTemplate> {

    private static final Logger log = LoggerFactory.getLogger(MapReduceTemplate.class);

    @Override
    public ServiceType getServiceType() { return ServiceType.MAPREDUCE; }

    public RoleAssigner<MapReduceTemplate> assignRole(MapReduceRoleType role) {
        return assignRole(role.name());
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
    
    protected String hdfsServiceName;
    public MapReduceTemplate useHdfs(String serviceName) {
        hdfsServiceName = serviceName;
        return this;
    }

    protected String jobtrackerLocalDataDir;
    public MapReduceTemplate useJobtrackerLocalDataDir(String jobtrackerLocalDataDir) {
        this.jobtrackerLocalDataDir = jobtrackerLocalDataDir;
        return this;
    }

    protected boolean enableMetrics=false;
    public MapReduceTemplate enableMetrics() {
        enableMetrics = true;
        return this;
    }

    @Override
    protected void preServiceAddChecks(ClouderaRestCaller caller) {
        if (hdfsServiceName==null) {
            List<String> hdfss = caller.findServicesOfType(clusterName, ServiceType.HDFS);
            if (hdfss.isEmpty()) throw new IllegalStateException("HDFS cluster required before can start MAPREDUCE");
            hdfsServiceName = hdfss.iterator().next();
        }
        
        if (jobtrackerLocalDataDir==null) {
            jobtrackerLocalDataDir = "/mnt/mapred/jt";
        }
        for (ServiceRoleHostInfo role: roles) {
            if (MapReduceRoleType.JOBTRACKER.toString().equalsIgnoreCase(role.type)) {
                String hostId = (String)role.hostRef.get("hostId");
                if (manager==null) {
                    log.warn("No manager connected to mapreduce; cannot automatically set up jobtracker dirs.");
                } else {
                    ClouderaCdhNode node = manager.findEntityForHostId(hostId);
                    if (node==null) {
                        log.warn("Manager "+manager+" does not know "+hostId+" for mapreduce; cannot automatically set up jobtracker dirs.");
                    } else {
                        String internalHostname = node.getAttribute(ClouderaCdhNode.PRIVATE_HOSTNAME);
                        log.debug("Initializing jobtracker dirs at "+hostId);
                        String jobtrackerDefaultDataDir = "/tmp/mapred/system";
                        int resultCode = node.newScript("creating jobtracker dirs at "+node+" for "+name).
                            body.append(
                                "sudo -u hdfs hdfs dfs -mkdir hdfs://"+internalHostname+jobtrackerLocalDataDir,
                                "sudo -u hdfs hdfs dfs -chown mapred:hadoop hdfs://"+internalHostname+jobtrackerLocalDataDir,
                                //not sure if above is needed, below definitely is needed
                                "sudo -u hdfs hdfs dfs -mkdir hdfs://"+internalHostname+jobtrackerDefaultDataDir,
                                "sudo -u hdfs hdfs dfs -chown mapred:hadoop hdfs://"+internalHostname+jobtrackerDefaultDataDir).
                            execute();
                        if (resultCode!=0) {
                            log.warn("Script failed initializing jobtracker dirs at "+hostId+" (return code "+resultCode+"); jobtracker startup may fail.");
                        }
                    }
                }
            }
        }
    }

    @Override
    protected Map<?, ?> convertConfig(Object config) {
        RestDataObjects.setConfig(config, "hdfs_service", hdfsServiceName);
        if (jobtrackerLocalDataDir!=null)
            RestDataObjects.setConfig(config, "jobtracker_mapred_local_dir_list", jobtrackerLocalDataDir);
        if (enableMetrics) {
            RestDataObjects.setMetricsRoleConfig(config, name, MapReduceRoleType.JOBTRACKER.name());
            RestDataObjects.setMetricsRoleConfig(config, name, MapReduceRoleType.TASKTRACKER.name());
        }
        Map<?,?> cfgOut = super.convertConfig(config);
        log.debug("MapReduce ${name} converted config: "+cfgOut);
        return cfgOut;
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
