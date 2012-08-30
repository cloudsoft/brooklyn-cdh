package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import io.cloudsoft.cloudera.rest.RestDataObjects.HBaseRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTemplate extends ServiceTemplate<HBaseTemplate> {

    private static final Logger log = LoggerFactory.getLogger(HBaseTemplate.class);

    @Override
    public ServiceType getServiceType() { return ServiceType.HBASE; }
    
    public RoleAssigner<HBaseTemplate> assignRole(HBaseRoleType role) {
        return assignRole(role.name());
    }

    public RoleAssigner<HBaseTemplate> assignRoleMaster() {
        return assignRole(HBaseRoleType.MASTER);
    }
    public RoleAssigner<HBaseTemplate> assignRoleRegionServer() {
        return assignRole(HBaseRoleType.REGIONSERVER);
    }
    public RoleAssigner<HBaseTemplate> assignRoleGateway() {
        return assignRole(HBaseRoleType.GATEWAY);
    }

    String hdfsServiceName;
    public HBaseTemplate useHdfs(String serviceName) {
        hdfsServiceName = serviceName;
        return this;
    }

    String zookeeperServiceName;
    public HBaseTemplate useZookeeper(String serviceName) {
        zookeeperServiceName = serviceName;
        return this;
    }

    @Override
    protected void preServiceAddChecks(ClouderaRestCaller caller) {
        if (hdfsServiceName==null) {
            List<String> hdfss = caller.findServicesOfType(clusterName, ServiceType.HDFS);
            if (hdfss.isEmpty()) throw new IllegalStateException("HDFS cluster required before can start ZOOKEEPER");
            hdfsServiceName = hdfss.iterator().next();
        }
        if (zookeeperServiceName==null) {
            List<String> zks = caller.findServicesOfType(clusterName, ServiceType.ZOOKEEPER);
            if (zks.isEmpty()) throw new IllegalStateException("Zookeeper cluster required before can start ZOOKEEPER");
            zookeeperServiceName = zks.iterator().next();
        }
    }

    @Override
    protected Map<?, ?> convertConfig(Object config) {
        RestDataObjects.setConfig(config, "hdfs_service", hdfsServiceName);
        RestDataObjects.setConfig(config, "zookeeper_service", zookeeperServiceName);
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

        System.out.println(new HBaseTemplate().
                hosts(H1, H2, H3).
                assignRoleMaster().toAnyHost().
                assignRoleRegionServer().toAllHosts().
            build(caller));
    }


}
