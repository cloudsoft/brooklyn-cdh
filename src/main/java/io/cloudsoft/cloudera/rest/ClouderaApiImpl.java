package io.cloudsoft.cloudera.rest;

import java.util.List;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiCommand;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiHostList;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiRoleList;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceList;
import com.cloudera.api.v1.CommandsResource;
import com.cloudera.api.v2.ClustersResourceV2;
import com.cloudera.api.v2.HostsResourceV2;
import com.cloudera.api.v2.RolesResourceV2;
import com.cloudera.api.v2.RootResourceV2;
import com.cloudera.api.v2.ServicesResourceV2;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

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

    @Override
    public ApiService createService(String clusterName, String serviceName, ServiceType serviceType) {
        final ApiServiceList apiServiceList = new ApiServiceList();
        ApiService service = new ApiService();
        service.setName(serviceName);
        service.setType(serviceType.name());
        apiServiceList.setServices(Lists.newArrayList(service));
        return Iterables.getOnlyElement(createServices(clusterName, apiServiceList));
    }

    @Override
    public List<ApiService> createServices(String clusterName, ApiServiceList apiServiceList) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ServicesResourceV2 servicesResource = v2.getClustersResource().getServicesResource(clusterName);
        return servicesResource.createServices(apiServiceList).getServices();
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
    

    @Override
    public ApiCluster addCluster(final String name) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        List<ApiCluster> clusters = Lists.newArrayList();
        ApiCluster apiCluster = new ApiCluster();
        apiCluster.setName(name);
        clusters.add(apiCluster);
        ApiClusterList apiClusters = new ApiClusterList(clusters);
        ApiClusterList apiClusterList = clustersResource.createClusters(apiClusters);
        if(apiClusterList.getClusters() == null) {
            throw new IllegalStateException("Cannot add a new cluster list named " + name);
        }
        return apiCluster;
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
    public List<ApiService> findServicesOfType(String clusterName, final ServiceType type) {
        return Lists.newArrayList(Iterables.filter(listServices(clusterName), new Predicate<ApiService>() {
            public boolean apply(@Nullable ApiService service) {
                return service != null && service.getType().equalsIgnoreCase(type.name());
            }
        }));
    }

    @Override
    public List<ApiRole> listRoles(String clusterName, String serviceName) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        RolesResourceV2 rolesResource = servicesResource.getRolesResource(serviceName);
        return rolesResource.readRoles().getRoles();
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
    public List<ApiRole> createRolesInService(String clusterName, String serviceName, List<ApiRole> roles) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        RolesResourceV2 rolesResource = servicesResource.getRolesResource(serviceName);

        ApiRoleList apiRoleList = new ApiRoleList(roles);
        return rolesResource.createRoles(apiRoleList).getRoles();
    }

    @Override
    public ApiRole getServiceRole(String clusterName, String serviceName, String roleName) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        RolesResourceV2 rolesResource = servicesResource.getRolesResource(serviceName); 
        return rolesResource.readRole(roleName);       
    }

    @Override
    public ApiCommand invokeServiceCommand(String clusterName, String serviceName, Command command) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        ClustersResourceV2 clustersResource = v2.getClustersResource();
        ServicesResourceV2 servicesResource = clustersResource.getServicesResource(clusterName);
        if(command.equals(Command.START)) {
            return servicesResource.startCommand(serviceName);
        } else if(command.equals(Command.STOP)) {
            return servicesResource.stopCommand(serviceName);
        } else if(command.equals(Command.RESTART)) {
            return servicesResource.restartCommand(serviceName);
        } else {
            throw new IllegalArgumentException("Command " + command + " is not supported");
        }
    }

    @Override
    public boolean isCommandSuccessful(long commandId) {
        RootResourceV2 v2 = apiRootResource.getRootV2();
        CommandsResource commandsResource = v2.getCommandsResource();
        return commandsResource.readCommand(commandId).getSuccess();
    }

}
