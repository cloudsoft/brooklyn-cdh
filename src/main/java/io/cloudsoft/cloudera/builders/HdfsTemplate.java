package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.util.text.Strings;

public class HdfsTemplate extends ServiceTemplate<HdfsTemplate> {

    private static final Logger log = LoggerFactory.getLogger(HdfsTemplate.class);

    @Override
    public ServiceType getServiceType() { return ServiceType.HDFS; }
    
    public RoleAssigner<HdfsTemplate> assignRole(HdfsRoleType role) {
        return assignRole(role.name());
    }

    protected boolean formatNameNodes = false;
    public HdfsTemplate formatNameNodes() {
        formatNameNodes = true;
        return this;
    }

    protected boolean enableMetrics=false;
    public HdfsTemplate enableMetrics() {
        return enableMetrics(true);
    }
    public HdfsTemplate enableMetrics(boolean metricsEnabled) {
        enableMetrics = metricsEnabled;
        return this;
    }

    public RoleAssigner<HdfsTemplate> assignRoleNameNode() {
        return assignRole(HdfsRoleType.NAMENODE);
    }
    public RoleAssigner<HdfsTemplate> assignRoleDataNode() {
        return assignRole(HdfsRoleType.DATANODE);
    }
    public RoleAssigner<HdfsTemplate> assignRoleSecondaryNameNode() {
        return assignRole(HdfsRoleType.SECONDARYNAMENODE);
    }

    protected boolean startOnceBuilt(ClouderaRestCaller caller) {
        caller.invokeHdfsFormatNameNodes(clusterName, name).block(60*1000);
        return super.startOnceBuilt(caller);
    }
    
    @Override
    protected Map<?, ?> convertConfig(Object config) {
        if (enableMetrics) {
            RestDataObjects.setMetricsRoleConfig(config, name, HdfsRoleType.DATANODE.name());
            RestDataObjects.setMetricsRoleConfig(config, name, HdfsRoleType.NAMENODE.name());
        }
        Map<?,?> cfgOut = super.convertConfig(config);
        log.debug("HDFS ${name} converted config: "+cfgOut);
        return cfgOut;
    }

    /*
ec2-184-73-4-212.compute-1.amazonaws.com

ip-10-147-154-47.ec2.internal
ip-10-145-233-130.ec2.internal
ip-10-152-163-89.ec2.internal
ip-10-147-152-224.ec2.internal

{"items":[{"name":"hdfs1-NAMENODE-1b29c31c6df5f6560ff84ab7a9a33fe6",
    "type":"NAMENODE","commissionState":"COMMISSIONED",
    "roleState":"STARTED",
    "serviceRef":{"serviceName":"hdfs1","clusterName":"cluster-JFmg23"},
    "hostRef":{"hostId":"ip-10-151-0-212.ec2.internal"},
    "maintenanceMode":false,"maintenanceOwners":[],"healthSummary":"GOOD",
    "healthChecks":[],"configStale":false,
    "roleUrl":"http://ip-10-151-21-80.ec2.internal:7180/cmf/roleRedirect/hdfs1-NAMENODE-1b29c31c6df5f6560ff84ab7a9a33fe6"},
{"name":"hdfs1-DATANODE-a6bc2d56b534229d9cdd7710de5d5ab0","type":"DATANODE","commissionState":"COMMISSIONED","roleState":
    "STARTED","serviceRef":{"serviceName":"hdfs1","clusterName":"cluster-JFmg23"},
    "hostRef":{"hostId":"ip-10-151-4-226.ec2.internal"},"maintenanceMode":false,"maintenanceOwners":[],"healthSummary":"GOOD",
    "healthChecks":[],"configStale":false,"roleUrl":"http://ip-10-151-21-80.ec2.internal:7180/cmf/roleRedirect/hdfs1-DATANODE-a6bc2d56b534229d9cdd7710de5d5ab0"},
{"name":"hdfs1-DATANODE-4f49f868501b7b3c24ceb06d2e9b09ec","type":"DATANODE","commissionState":"COMMISSIONED","roleState":"STARTED","serviceRef":{"serviceName":"hdfs1","clusterName":"cluster-JFmg23"},"hostRef":{"hostId":"ip-10-151-21-87.ec2.internal"},"maintenanceMode":false,"maintenanceOwners":[],"healthSummary":"GOOD","healthChecks":[],"configStale":false,"roleUrl":"http://ip-10-151-21-80.ec2.internal:7180/cmf/roleRedirect/hdfs1-DATANODE-4f49f868501b7b3c24ceb06d2e9b09ec"},
{"name":"hdfs1-DATANODE-1b29c31c6df5f6560ff84ab7a9a33fe6","type":"DATANODE","commissionState":"COMMISSIONED","roleState":"STARTED","serviceRef":{"serviceName":"hdfs1","clusterName":"cluster-JFmg23"},"hostRef":{"hostId":"ip-10-151-0-212.ec2.internal"},"maintenanceMode":false,"maintenanceOwners":[],"healthSummary":"GOOD","healthChecks":[],"configStale":false,"roleUrl":"http://ip-10-151-21-80.ec2.internal:7180/cmf/roleRedirect/hdfs1-DATANODE-1b29c31c6df5f6560ff84ab7a9a33fe6"},
{"name":"hdfs1-DATANODE-2eb5acf317e00087975619c0d18bda90","type":"DATANODE","commissionState":"COMMISSIONED","roleState":"STARTED","serviceRef":{"serviceName":"hdfs1","clusterName":"cluster-JFmg23"},"hostRef":{"hostId":"ip-10-151-20-177.ec2.internal"},"maintenanceMode":false,"maintenanceOwners":[],"healthSummary":"GOOD","healthChecks":[],"configStale":false,"roleUrl":"http://ip-10-151-21-80.ec2.internal:7180/cmf/roleRedirect/hdfs1-DATANODE-2eb5acf317e00087975619c0d18bda90"},
{"name":"hdfs1-SECONDARYNAMENODE-2eb5acf317e00087975619c0d18bda90","type":"SECONDARYNAMENODE","commissionState":"COMMISSIONED","roleState":"STARTED","serviceRef":{"serviceName":"hdfs1","clusterName":"cluster-JFmg23"},"hostRef":{"hostId":"ip-10-151-20-177.ec2.internal"},"maintenanceMode":false,"maintenanceOwners":[],"healthSummary":"GOOD","healthChecks":[],"configStale":false,"roleUrl":"http://ip-10-151-21-80.ec2.internal:7180/cmf/roleRedirect/hdfs1-SECONDARYNAMENODE-2eb5acf317e00087975619c0d18bda90"}]}

http://ec2-54-242-139-26.compute-1.amazonaws.com:7180/api/v2/clusters/cluster-JFmg23/services/hdfs-B9Sv/roles : 
[body:[items:[[name:namenode-ip-10-151-0-212-ec2-internal, type:NAMENODE, hostRef:[hostId:ip-10-151-0-212.ec2.internal]], 
[name:secondarynamenode-ip-10-151-20-177-ec2-internal, type:SECONDARYNAMENODE, hostRef:[hostId:ip-10-151-20-177.ec2.internal]], [name:datanode-ip-10-151-0-212-ec2-internal, type:DATANODE, hostRef:[hostId:ip-10-151-0-212.ec2.internal]], [name:datanode-ip-10-151-20-177-ec2-internal, type:DATANODE, hostRef:[hostId:ip-10-151-20-177.ec2.internal]], [name:datanode-ip-10-151-21-87-ec2-internal, type:DATANODE, hostRef:[hostId:ip-10-151-21-87.ec2.internal]]]], requestContentType:application/json]

http://ec2-54-242-139-26.compute-1.amazonaws.com:7180/api/v2/clusters/cluster-JFmg23/services/hdfs-xlck/roles : 
[body:[items:[

[name:namenode-ip-10-151-0-212-ec2-internal, type:NAMENODE, 
hostRef:[hostId:ip-10-151-0-212.ec2.internal]], 

[name:secondarynamenode-ip-10-151-20-177-ec2-internal, type:SECONDARYNAMENODE, hostRef:[hostId:ip-10-151-20-177.ec2.internal]], [name:datanode-ip-10-151-0-212-ec2-internal, type:DATANODE, hostRef:[hostId:ip-10-151-0-212.ec2.internal]], [name:datanode-ip-10-151-20-177-ec2-internal, type:DATANODE, hostRef:[hostId:ip-10-151-20-177.ec2.internal]], [name:datanode-ip-10-151-21-87-ec2-internal, type:DATANODE, hostRef:[hostId:ip-10-151-21-87.ec2.internal]]]], requestContentType:application/json]

2012-12-14 17:02:42,557 INFO  POST 
http://ec2-184-73-4-212.compute-1.amazonaws.com:7180/api/v2/clusters/cluster-Pb1tF0/services/hdfs-QdOn/roles : 
[requestContentType:application/json] (Thread[main,5,main])

{"items":[
  {"name":"namenode-ip-10-147-154-47-ec2-internal","type":"NAMENODE","hostRef":
    {"hostId":"ip-10-147-154-47.ec2.internal"}},
  {"name":"secondarynamenode-ip-10-145-233-130-ec2-internal","type":"SECONDARYNAMENODE","hostRef":
    {"hostId":"ip-10-145-233-130.ec2.internal"}},
  {"name":"datanode-ip-10-147-154-47-ec2-internal","type":"DATANODE","hostRef":
    {"hostId":"ip-10-147-154-47.ec2.internal"}},
  {"name":"datanode-ip-10-145-233-130-ec2-internal","type":"DATANODE","hostRef":
    {"hostId":"ip-10-145-233-130.ec2.internal"}},
  {"name":"datanode-ip-10-152-163-89-ec2-internal","type":"DATANODE","hostRef":
    {"hostId":"ip-10-152-163-89.ec2.internal"}}
]}


curl -X POST -H "Content-Type:application/json" -u admin:admin \
  -d '{"items":[ 
  {"name":"ABCD-namenode-ip-10-147-154-47-ec2-internal","type":"NAMENODE","hostRef": 
    {"hostId":"ip-10-147-154-47.ec2.internal"}}, 
  {"name":"secondarynamenode-ip-10-145-233-130-ec2-internal","type":"SECONDARYNAMENODE","hostRef": 
    {"hostId":"ip-10-145-233-130.ec2.internal"}}, 
  {"name":"datanode-ip-10-147-154-47-ec2-internal","type":"DATANODE","hostRef": 
    {"hostId":"ip-10-147-154-47.ec2.internal"}}, 
  {"name":"datanode-ip-10-145-233-130-ec2-internal","type":"DATANODE","hostRef": 
    {"hostId":"ip-10-145-233-130.ec2.internal"}}, 
  {"name":"datanode-ip-10-152-163-89-ec2-internal","type":"DATANODE","hostRef": 
    {"hostId":"ip-10-152-163-89.ec2.internal"}} 
]}' \
  http://ec2-184-73-4-212.compute-1.amazonaws.com:7180/api/v2/clusters/cluster-Pb1tF0/services/hdfs-QdOn/roles
   
{"items": [
        { "name": "master1", "type": "MASTER", "hostRef": { "hostId": "localhost" } },
        { "name": "rs1", "type": "REGIONSERVER", "hostRef": { "hostId": "localhost" } } ] }' \


     */
    public static void main(String[] args) {
        String SERVER = "ec2-184-73-4-212.compute-1.amazonaws.com",
                H1 = "ip-10-147-154-47.ec2.internal",
                H2 = "ip-10-145-233-130.ec2.internal",  
                H3 = "ip-10-152-163-89.ec2.internal",
                H4 = "ip-10-147-152-224.ec2.internal";
        
        ClouderaRestCaller caller = ClouderaRestCaller.newInstance(SERVER, "admin", "admin");

//        Object x = caller.getServiceRolesJson("cluster-JFmg23", "hdfs1");
//        System.out.println(x);
        
        Object hdfsService = new HdfsTemplate().
                named("hdfs-"+Strings.makeRandomId(4)).
                hosts(H1, H2, H3).
                assignRole(HdfsRoleType.NAMENODE).to(H1).
                assignRole(HdfsRoleType.SECONDARYNAMENODE).to(H2).
                assignRole(HdfsRoleType.DATANODE).toAllHosts().
                formatNameNodes().
            build(caller);
        
//        Object hdfsService2 = new HdfsTemplate().
//                named("hdfs-1").
//                abortIfServiceExists().
//                cluster("My Cluster").
//                hosts(H1, H2, H3).
//                assignRoleNameNode().toAnyHost().
//                assignRoleSecondaryNameNode().toAnyHost().
//                assignRoleDataNode().toAllHosts().
//                formatNameNodes().
//            build(caller);
    }


/*  Hadoop Metrics2 Safety Valve
*.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink
 
datanode.sink.file.filename=/tmp/datanode-metrics.out
*/
}
