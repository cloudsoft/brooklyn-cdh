package io.cloudsoft.cloudera.rest;

import java.util.List;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiHostList;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceList;
import com.cloudera.api.v2.ClustersResourceV2;
import com.cloudera.api.v2.HostsResourceV2;
import com.cloudera.api.v2.RolesResourceV2;
import com.cloudera.api.v2.RootResourceV2;
import com.cloudera.api.v2.ServicesResourceV2;

public class ClouderaApiImpl implements ClouderaApi {
    
    private final ApiRootResource apiRootResource;
    
    public ClouderaApiImpl(String host, String user, String password) {
        this.apiRootResource = new ClouderaManagerClientBuilder().withHost(host)
                                                                 .withUsernamePassword(user, password)
                                                                 .build();
    }

    /* (non-Javadoc)
     * @see io.cloudsoft.cloudera.rest.ClouderaApi#listHosts()
     */
    @Override
    public List<ApiHost> listHosts() {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        HostsResourceV2 hostsResource = v2.getHostsResource();
        ApiHostList apiHostList = hostsResource.readHosts(DataView.FULL);
        return apiHostList.getHosts();
    }
    
    /* (non-Javadoc)
     * @see io.cloudsoft.cloudera.rest.ClouderaApi#listClusters()
     */
    @Override
    public List<ApiCluster> listClusters() {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ApiClusterList apiClusterList = clustersResource.readClusters(DataView.FULL);
        return apiClusterList.getClusters();
    }
    
    /* (non-Javadoc)
     * @see io.cloudsoft.cloudera.rest.ClouderaApi#listServices(java.lang.String)
     */
    @Override
    public List<ApiService> listServices(String clusterName) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        ApiServiceList apiServiceList = servicesResource.readServices(DataView.FULL);
        return apiServiceList.getServices();
    }

    @Override
    public ApiService getService(String clusterName, String serviceName) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        return servicesResource.readService(serviceName);
    }

    @Override
    public List<ApiRole> listServiceRoles(String clusterName, String serviceName) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        RolesResourceV2 rolesResource = servicesResource.getRolesResource(serviceName); 
        return rolesResource.readRoles().getRoles();
    }

    @Override
    public ApiRole getServiceRole(String clusterName, String serviceName, String roleName) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        RolesResourceV2 rolesResource = servicesResource.getRolesResource(serviceName); 
        return rolesResource.readRole(roleName);       
    }

    
}
