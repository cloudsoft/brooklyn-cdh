package io.cloudsoft.cloudera.builders;

import com.cloudera.api.model.ApiService;
import com.google.common.collect.Iterables;
import io.cloudsoft.cloudera.rest.ClouderaApi;
import io.cloudsoft.cloudera.rest.RestDataObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class HBaseTemplate extends ServiceTemplate<HBaseTemplate> {

    private static final Logger log = LoggerFactory.getLogger(HBaseTemplate.class);

    @Override
    public ClouderaApi.ServiceType getServiceType() { return ClouderaApi.ServiceType.HBASE; }
    
    public RoleAssigner<HBaseTemplate> assignRole(ClouderaApi.HBaseRoleType role) {
        return assignRole(role.name());
    }

    public RoleAssigner<HBaseTemplate> assignRoleMaster() {
        return assignRole(ClouderaApi.HBaseRoleType.MASTER);
    }
    public RoleAssigner<HBaseTemplate> assignRoleRegionServer() {
        return assignRole(ClouderaApi.HBaseRoleType.REGIONSERVER);
    }
    public RoleAssigner<HBaseTemplate> assignRoleGateway() {
        return assignRole(ClouderaApi.HBaseRoleType.GATEWAY);
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
    protected void preServiceAddChecks(ClouderaApi api) {
        if (hdfsServiceName==null) {
            List<ApiService> hdfss = api.findServicesOfType(clusterName, ClouderaApi.ServiceType.HDFS);
            if (hdfss.isEmpty()) throw new IllegalStateException("HDFS cluster required before can start ZOOKEEPER");
            hdfsServiceName = Iterables.getOnlyElement(hdfss).getName();
        }
        if (zookeeperServiceName==null) {
            List<ApiService> zks = api.findServicesOfType(clusterName, ClouderaApi.ServiceType.ZOOKEEPER);
            if (zks.isEmpty()) throw new IllegalStateException("Zookeeper cluster required before can start ZOOKEEPER");
            zookeeperServiceName = Iterables.getOnlyElement(zks).getName();
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
    
}
