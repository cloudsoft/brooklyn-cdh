package io.cloudsoft.cloudera.rest;

import java.util.List;

import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiService;

public interface ClouderaApi {

    public abstract List<ApiHost> listHosts();

    public abstract List<ApiCluster> listClusters();

    public abstract List<ApiService> listServices(String clusterName);
    public abstract ApiService getService(String clusterName, String serviceName);
    
    public abstract List<ApiRole> listServiceRoles(String clusterName, String serviceName);

    public abstract ApiRole getServiceRole(String clusterName, String serviceName, String roleName);

}