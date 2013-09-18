package io.cloudsoft.cloudera.rest;

import java.util.List;

import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiCommand;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceList;

public interface ClouderaApi {

    public enum Command {
        START, STOP, RESTART
    }

    enum ClusterType { CDH3, CDH4 }
    enum ServiceType { HDFS, MAPREDUCE, HBASE, OOZIE, ZOOKEEPER, HUE, YARN }

    enum HdfsRoleType { DATANODE, NAMENODE, SECONDARYNAMENODE, BALANCER, GATEWAY, HTTPFS, FAILOVERCONTROLLER }
    enum MapReduceRoleType { JOBTRACKER, TASKTRACKER, GATEWAY }
    enum HBaseRoleType { MASTER, REGIONSERVER, GATEWAY }
    enum ZookeeperRoleType { SERVER }

    public abstract List<ApiCluster> listClusters();
    public abstract ApiCluster addCluster(String name);

    public abstract List<ApiHost> listHosts();

    public abstract ApiService createService(String clusterName, String serviceName, ServiceType serviceType);
    public abstract List<ApiService> createServices(String clusterName, ApiServiceList services);
    public abstract List<ApiService> listServices(String clusterName);
    public abstract ApiService getService(String clusterName, String serviceName);
    public abstract List<ApiService> findServicesOfType(String clusterName, ServiceType type);

    public abstract List<ApiRole> listRoles(String clusterName, String serviceName);
    public abstract List<ApiRole> listServiceRoles(String clusterName, String serviceName);
    public abstract List<ApiRole> createRolesInService(String clusterName, String serviceName, List<ApiRole> roles);
    public abstract ApiRole getServiceRole(String clusterName, String serviceName, String roleName);

    public abstract ApiCommand invokeServiceCommand(String clusterName, String serviceName, Command command);
    public abstract boolean isCommandSuccessful(long commandId);

}