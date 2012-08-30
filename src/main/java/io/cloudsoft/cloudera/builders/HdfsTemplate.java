package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceRoleHostInfo;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;

import java.util.List;
import java.util.Map;

import brooklyn.util.IdGenerator;

public class HdfsTemplate extends ServiceTemplate<HdfsTemplate> {

    public RoleAssigner<HdfsTemplate> assignRole(HdfsRoleType role) {
        return assignRole(role.name());
    }

    protected boolean abortIfServiceExists = false;
    public HdfsTemplate abortIfServiceExists() {
        abortIfServiceExists = true;
        return this;
    }

    protected boolean formatNameNodes = false;
    public HdfsTemplate formatNameNodes() {
        formatNameNodes = true;
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

    @Override
    public Object build(ClouderaRestCaller caller) {
        if (name==null) name = "hdfs-"+IdGenerator.makeRandomId(8);
        
        List<String> clusters = caller.getClusters();
        if (clusterName==null) {
            if (!clusters.isEmpty()) clusterName = clusters.iterator().next();
            else clusterName = "cluster-"+IdGenerator.makeRandomId(6);
        }
        if (!clusters.contains(clusterName)) caller.addCluster(clusterName);

        List<String> services = caller.getServices(clusterName);
        if (abortIfServiceExists && services.contains(name))
            return true;
        
        caller.addService(clusterName, name, ServiceType.HDFS);
        caller.addServiceRoleHosts(clusterName, name, roles.toArray(new ServiceRoleHostInfo[0]));
        
        Object config = caller.getServiceConfig(clusterName, name);
        Map<?,?> cfgOut = RestDataObjects.convertConfigForSetting(config, clusterName+"-"+name);
        caller.setServiceConfig(clusterName, name, cfgOut);

        if (formatNameNodes) {
            caller.invokeHdfsFormatNameNodes(clusterName, name).block(60*1000);
        }
        
        return caller.invokeServiceCommand(clusterName, name, "start").block(60*1000);
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


}
