package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        enableMetrics = true;
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

    
//    public static void main(String[] args) {
//        String SERVER = "ec2-23-22-170-157.compute-1.amazonaws.com",
//            H1 = "ip-10-202-94-94.ec2.internal",
//            H2 = "ip-10-196-119-79.ec2.internal",
//            H3 = "ip-10-118-190-146.ec2.internal";
//        
//        ClouderaRestCaller caller = ClouderaRestCaller.newInstance(SERVER, "admin", "admin");
//
////        Object hdfsService = new HdfsTemplate().
////                named("hdfs-1").
////                hosts(H1, H2, H3).
////                assignRole(HdfsRoleType.NAMENODE).to(H1).
////                assignRole(HdfsRoleType.SECONDARYNAMENODE).to(H2).
////                assignRole(HdfsRoleType.DATANODE).toAllHosts().
////                formatNameNodes().
////            build(caller);
//        
////        Object hdfsService2 = new HdfsTemplate().
////                named("hdfs-1").
////                abortIfServiceExists().
////                cluster("My Cluster").
////                hosts(H1, H2, H3).
////                assignRoleNameNode().toAnyHost().
////                assignRoleSecondaryNameNode().toAnyHost().
////                assignRoleDataNode().toAllHosts().
////                formatNameNodes().
////            build(caller);
//    }


/*  Hadoop Metrics2 Safety Valve
*.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink
 
datanode.sink.file.filename=/tmp/datanode-metrics.out
*/
}
