package io.cloudsoft.cloudera.builders;

import brooklyn.entity.proxying.EntitySpec;
import brooklyn.util.exceptions.Exceptions;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiService;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaService;
import io.cloudsoft.cloudera.rest.ClouderaApi;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.text.Identifiers;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public abstract class ServiceTemplate<T extends ServiceTemplate<?>> extends AbstractTemplate<T> {

    private static final Logger log = LoggerFactory.getLogger(ServiceTemplate.class);

    protected String clusterName;
    protected ClouderaManagerNode manager;
    protected Set<String> hostIds = new LinkedHashSet<String>();
    protected List<ApiRole> roles = Lists.newArrayList();

    @SuppressWarnings("unchecked")
    public T cluster(String clusterName) {
        this.clusterName = clusterName;
        return (T)this;
    }
    public String getClusterName() {
        return clusterName;
    }

    @SuppressWarnings("unchecked")
    public T manager(ClouderaManagerNode manager) {
        this.manager = manager;
        return (T)this;
    }
    @SuppressWarnings("unchecked")
    public T discoverHostsFromManager() {
        Preconditions.checkNotNull(manager, "manager must be specified before can discover hosts");
        hosts(manager.getAttribute(ClouderaManagerNode.MANAGED_HOSTS));
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public T hosts(String ...hostIds) {
        Collections.addAll(this.hostIds, hostIds);
        return (T)this;
    }
    @SuppressWarnings("unchecked")
    public T hosts(Collection<String> hostIds) {
        for (Object hostId: hostIds) {
            // TODO convert from entities etc
            this.hostIds.add(""+hostId);
        }
        return (T)this;
    }

    protected boolean abortIfServiceExists = false;
    @SuppressWarnings("unchecked")
    public T abortIfServiceExists() {
        abortIfServiceExists = true;
        return (T)this;
    }

    private int roleIndex=0;
    public class RoleAssigner<U> {
        String type;

        public RoleAssigner(String roleType) {
            this.type = roleType;
        }

        @SuppressWarnings("unchecked")
        public U to(String host) {
            apply(Arrays.asList(host));
            return (U)ServiceTemplate.this;
        }

        @SuppressWarnings("unchecked")
        public U toAllHosts() {
            if (ServiceTemplate.this.hostIds.isEmpty())
                throw new IllegalStateException("No hosts defined, when defining "+ServiceTemplate.this);
            apply(ServiceTemplate.this.hostIds);
            return (U)ServiceTemplate.this;
        }

        public U toAnyHost() {
            if (ServiceTemplate.this.hostIds.isEmpty())
                throw new IllegalStateException("No hosts defined, when defining "+ServiceTemplate.this);
            if (roleIndex >= ServiceTemplate.this.hostIds.size()) roleIndex = 0;
            int ri = roleIndex;
            roleIndex++;
            Iterator<String> hi = ServiceTemplate.this.hostIds.iterator();
            while (ri>0) {
                hi.next();
                ri--;
            }
            return to(hi.next());
        }

        void apply(Collection<String> hosts) {
            for (String host: hosts) {
                hostIds.add(host);
                ServiceTemplate.this.roles.add(ServiceRoleHostInfo.newInstance(getName(), type, host));
            }
        }
    }

    //List<ServiceRoleHostInfo> roles = new ArrayList<ServiceRoleHostInfo>();

    public RoleAssigner<T> assignRole(String role) {
        return new RoleAssigner<T>(role);
    }

    @SuppressWarnings("unchecked")
    public T useDefaultName() {
        if (name==null) name = getServiceType().name().toLowerCase()+"-"+Identifiers.makeRandomId(8);
        return (T)this;
    }

    public abstract ClouderaApi.ServiceType getServiceType();
    
    @Override
    public Boolean build(ClouderaApi api) {
        if (name==null) useDefaultName();
        
        List<ApiCluster> clusters = api.listClusters();
        if (clusterName==null) {
            if (!clusters.isEmpty())
                clusterName = Iterables.get(clusters, 0).getName();
            else
                clusterName = "cluster-"+Identifiers.makeRandomId(6);
        }
        Optional<ApiCluster> apiClusterOptional = Iterables.tryFind(clusters, new Predicate<ApiCluster>() {
            public boolean apply(@Nullable ApiCluster input) {
                return input != null && input.getName().equals(clusterName);
            }
        });
        if (!apiClusterOptional.isPresent()) {
            api.addCluster(clusterName);
        }

        List<ApiService> services = api.listServices(clusterName);
        Optional<ApiService> apiServiceOptional = Iterables.tryFind(services, new Predicate<ApiService>() {
            public boolean apply(@Nullable ApiService input) {
                return input != null && input.getName().equals(name);
            }
        });

        if (abortIfServiceExists && apiServiceOptional.isPresent()) {
            return true;
        }

        preServiceAddChecks(api);
       
        api.addService(clusterName, name, getServiceType());
        api.addServiceRoleHosts(clusterName, name, roles.toArray(new ServiceRoleHostInfo[roles.size()]));
        
        Object config = api.getServiceConfig(clusterName, name);
        Map<?,?> cfgOut = convertConfig(config);
        api.setServiceConfig(clusterName, name, cfgOut);

        return startOnceBuilt(api);
    }

    protected void preServiceAddChecks(ClouderaApi api) {
    }
    
    protected boolean startOnceBuilt(ClouderaApi api) {
        return api.invokeServiceCommand(clusterName, name, "start").block(60*1000);
    }

    protected Map<?, ?> convertConfig(Object config) {
        log.debug("Config for CDH "+clusterName+"-"+name+" is: "+config);
        return RestDataObjects.convertConfigForSetting(config, clusterName+"-"+name);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ClouderaService buildWithEntity(Map flags, Entity owner) {
        MutableMap flags2 = MutableMap.copyOf(flags);
        flags2.put("template", this);
        if (manager!=null) flags2.put("manager", manager);
        String name = (String) flags2.get("name");
        if (name == null) {
            useDefaultName();
            name = this.name;
            flags2.put("name", name);
        }
        ClouderaService result = ((EntityInternal)owner).getManagementSupport().getManagementContext().getEntityManager()
                .createEntity(EntitySpec.create(ClouderaService.class).configure(flags2));
        owner.addChild(result);
        Entities.manage(result);
        result.create();
        try { 
            // pause a bit after creation, to ensure it really is created
            // (not needed, i don't think...)
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
        return result;
    }
    
    @SuppressWarnings("rawtypes")
    public ClouderaService buildWithEntity(Entity owner) { return buildWithEntity(new MutableMap(), owner); }

}
