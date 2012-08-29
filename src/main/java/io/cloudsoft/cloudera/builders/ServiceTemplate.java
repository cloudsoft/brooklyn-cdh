package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceRoleHostInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


public abstract class ServiceTemplate<T extends ServiceTemplate<?>> extends AbstractTemplate<T> {
    
    Set<String> hostIds = new LinkedHashSet<String>();
    @SuppressWarnings("unchecked")
    public T hosts(String ...hostIds) {
        for (String hostId: hostIds) 
            this.hostIds.add(hostId);
        return (T)this;
    }
    public T hosts(List hostIds) {
        for (Object hostId: hostIds) {
            // TODO convert from entities etc
            this.hostIds.add(""+hostId);
        }
        return (T)this;
    }

    private int roleIndex;
    public class RoleAssigner<U> {
        String type;
        String name;
        
        public RoleAssigner(String roleType) {
            this.type = roleType;
        }
        
        public RoleAssigner<U> named(String name) {
            this.name = name;
            return this;
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

        @SuppressWarnings("unchecked")
        public U toAnyHost() {
            roleIndex++;
            if (ServiceTemplate.this.hostIds.isEmpty()) 
                throw new IllegalStateException("No hosts defined, when defining "+ServiceTemplate.this);
            if (roleIndex >= ServiceTemplate.this.hostIds.size()) roleIndex = 0;
            Iterator<String> hi = ServiceTemplate.this.hostIds.iterator();
            while (roleIndex>0) {
                hi.next();
                roleIndex--;
            }
            return to(hi.next());
        }

        void apply(Collection<String> hosts) {
            for (String host: hosts) {
                hostIds.add(host);
                ServiceTemplate.this.roles.add(
                        name!=null ? new ServiceRoleHostInfo(name, type, host) : new ServiceRoleHostInfo(type, host));
            }
        }
    }
    
    List<ServiceRoleHostInfo> roles = new ArrayList<ServiceRoleHostInfo>();
    
    public RoleAssigner<T> assignRole(String role) {
        return new RoleAssigner<T>(role);
    }

}
