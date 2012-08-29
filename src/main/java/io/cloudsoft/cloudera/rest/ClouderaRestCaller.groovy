package io.cloudsoft.cloudera.rest;

import groovyx.net.http.ContentType
import io.cloudsoft.cloudera.rest.RestDataObjects.ClusterAddInfo
import io.cloudsoft.cloudera.rest.RestDataObjects.ClusterType
import io.cloudsoft.cloudera.rest.RestDataObjects.HdfsRoleType;
import io.cloudsoft.cloudera.rest.RestDataObjects.HostAddInfo
import io.cloudsoft.cloudera.rest.RestDataObjects.RemoteCommand;
import io.cloudsoft.cloudera.rest.RestDataObjects.RemoteCommandSet;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceRoleHostInfo
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType
import brooklyn.util.IdGenerator

import com.google.common.base.Preconditions

/** requires server, authName, authPass in constructor */    
public class ClouderaRestCaller {

    public static ClouderaRestCaller newInstance(String server, String user, String pass) {
        return new ClouderaRestCaller(server:server, authName:user, authPass:pass);
    }
    
    String server;
    
    int port = 7180;
    String protocol = "http";
    
    String authName, authPass;

    RestCaller _caller;
    
    public synchronized RestCaller getCaller() {
        if (_caller!=null) return _caller;
        Preconditions.checkNotNull(server, "server");
        Preconditions.checkNotNull(authName, "server");
        Preconditions.checkNotNull(authPass, "server");
        _caller = new RestCaller(context: "Cloudera Manager at "+server,
            urlBase: ""+protocol+"://"+server+":"+port+"/api/v1/",
            authName: authName, authPass: authPass);
        return _caller;
    }

    public Object getHostsJson() {
        return caller.doGet("hosts")
    }
    public List<String> getHosts() {
        return getHostsJson().items.collect { it.hostname }
    }
    public List addHost(HostAddInfo host) { addHosts(host) }
    public List addHosts(HostAddInfo ...hosts) {
        def body = [items: (hosts as List).collect { it.asMap(false) } ]
        return caller.doPost("", path: "hosts", body: body, requestContentType: ContentType.JSON).items.collect { it.hostname }
    }
    
    public Object getClustersJson() {
        return caller.doGet("clusters")
    }
    public List<String> getClusters() {
        return getClustersJson().items.collect { it.name }
    }
    public List addCluster(String name, ClusterType version=ClusterType.CDH4) { addClusters(new ClusterAddInfo(name: name, version: version.name())) }
    public List addCluster(ClusterAddInfo cluster) { addClusters(cluster) }
    public List addClusters(ClusterAddInfo ...clusters) {
        def body = [items: (clusters as List).collect { it.asMap(false) } ]
        return caller.doPost("", path: "clusters", body: body, requestContentType: ContentType.JSON).items.collect { it.name }
    }

    public Object getServicesJson(String clusterName) {
        println "clusters/${URLParamEncoder.encode(clusterName)}/services"
        return caller.doGet("clusters/${URLParamEncoder.encode(clusterName)}/services")
    }
    public List<String> getServices(String clusterName) {
        return getServicesJson(clusterName).items.collect { it.name }
    }
    public List<String> addService(String clusterName, String serviceName, ServiceType serviceType) {
        def body = [items: [[name:serviceName, type:serviceType.name()]] ]
        return caller.doPost("clusters/${URLParamEncoder.encode(clusterName)}/", path: "services", body: body, requestContentType: ContentType.JSON).items.collect { it.name }
    }

    public Object getServiceRolesJson(String clusterName, String serviceName) {
        return caller.doGet("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/roles");
    }
    public Object getServiceRoles(String clusterName, String serviceName) {
        return getServiceRolesJson(clusterName, serviceName).items.collect { it.name }
    }
    public Object getServiceRoleTypesJson(String clusterName, String serviceName) {
        return caller.doGet("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/config");
    }
    public List<String> getServiceRoleTypes(String clusterName, String serviceName) {
        return getServiceRoleTypesJson(clusterName, serviceName).roleTypeConfigs.collect { it.roleType }
    }
    public List<String> addServiceRoleHosts(String clusterName, String serviceName, 
                ServiceRoleHostInfo ...rolehosts) {
        def body = [items: rolehosts.collect { ServiceRoleHostInfo h -> h.asMap() } ]
        println body
        return caller.doPost("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/", path: "roles", 
            body: body, requestContentType: ContentType.JSON).items.collect { it.name }
    }
    public RemoteCommand invokeServiceCommand(String clusterName, String serviceName, String command) {
        Object commandJson = caller.doPost("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/commands/", path: command);
        return RemoteCommand.fromJson(commandJson, this);
    }
    public Object getServiceRoleConfig(String clusterName, String serviceName, String roleName, boolean full=true) {
        return caller.doGet("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/"+
            "roles/${URLParamEncoder.encode(roleName)}/config",
            view: (full?"full":"summary"));
    }
    public Object getServiceConfig(String clusterName, String serviceName, boolean full=true) {
        return caller.doGet("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/config",
            view: (full?"full":"summary"));
    }
    public Object setServiceConfig(String clusterName, String serviceName, Map config) {
        return caller.doPut("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/config",
            body: config, requestContentType: ContentType.JSON);
    }

    public Object invokeRoleCommand(String clusterName, String serviceName, String roleCommand, Object body) {
        return caller.doPost("clusters/${URLParamEncoder.encode(clusterName)}/"+
            "services/${URLParamEncoder.encode(serviceName)}/"+
            "roleCommands/${URLParamEncoder.encode(roleCommand)}",
            body: body, requestContentType: ContentType.JSON);
    }
    public RemoteCommandSet invokeHdfsFormatNameNodes(String cluster, String service) {
        def rj = getServiceRolesJson(cluster, service);
        Object commands = invokeRoleCommand(cluster, service, "hdfsFormat",
            [items: rj.items.findAll({ it.type == HdfsRoleType.NAMENODE.name() }).collect { it.name } ]);
        return RemoteCommandSet.fromJson(commands, this);
    }

    public Object getCommandJson(Object commandId) {
        return caller.doGet("commands/${URLParamEncoder.encode(""+commandId)}");
    }

    public static void main(String[] args) {
        def SERVER = "ec2-23-22-170-157.compute-1.amazonaws.com";
        def H1 = "ip-10-202-94-94.ec2.internal";
        def H2 = "ip-10-196-119-79.ec2.internal";
        def H3 = "ip-10-118-190-146.ec2.internal";
        
        def caller = new ClouderaRestCaller(server: SERVER, authName:"admin", authPass:"admin");
        println caller.getHostsJson();
        
        def clusters = caller.getClusters();
        println clusters;
        if (!clusters) {
            clusters = caller.addCluster("Cluster2 CDH4");
            println "created cluster, now have: "+caller.getClustersJson();
        }
        def cluster = clusters.iterator().next();
        
        def services = caller.getServices(cluster);
        println "services: "+services;
        if (!services) {
            String service = "hdfs-"+IdGenerator.makeRandomId(4);
            services = caller.addService(cluster, service, ServiceType.HDFS);
            println "post-creation, services now: "+services;
        }
        def service = services.iterator().next(); 
        
        println "role types at ${service}: "+caller.getServiceRoleTypes(cluster, service);
        
        def roles = caller.getServiceRoles(cluster, service);
        if (!roles) {
            println "after roles added: "+caller.addServiceRoleHosts(cluster, service,
            new ServiceRoleHostInfo(HdfsRoleType.DATANODE, H1),
            new ServiceRoleHostInfo(HdfsRoleType.DATANODE, H2),
            new ServiceRoleHostInfo(HdfsRoleType.DATANODE, H3),
            new ServiceRoleHostInfo(HdfsRoleType.NAMENODE, H1),
            new ServiceRoleHostInfo(HdfsRoleType.SECONDARYNAMENODE, H2));
            roles = caller.getServiceRoles(cluster, service);
        }
        String role = roles.iterator().next()
        
        def cfg = caller.getServiceConfig(cluster, service);
        println "role configs at ${service}: "+cfg
        Map cfgOut = RestDataObjects.convertConfigForSetting(cfg, cluster+"-"+service);
        println "role configs to set at ${service}: "+cfgOut
        caller.setServiceConfig(cluster, service, cfgOut);
        
        caller.invokeHdfsFormatNameNodes(service, cluster).block(60*1000);
        
        caller.invokeServiceCommand(cluster, service, "start").block(60*1000);
        
    }

}
