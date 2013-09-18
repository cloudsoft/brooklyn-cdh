package io.cloudsoft.cloudera.builders;

import io.cloudsoft.cloudera.brooklynnodes.ClouderaCdhNode;
import io.cloudsoft.cloudera.rest.ClouderaApi;
import io.cloudsoft.cloudera.rest.ClouderaRestCaller;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceRoleHostInfo;
import io.cloudsoft.cloudera.rest.RestDataObjects.ServiceType;
import io.cloudsoft.cloudera.rest.RestDataObjects.ZookeeperRoleType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperTemplate extends ServiceTemplate<ZookeeperTemplate> {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperTemplate.class);
    
    @Override
    public ServiceType getServiceType() { return ServiceType.ZOOKEEPER; }
    
    public RoleAssigner<ZookeeperTemplate> assignRole(ZookeeperRoleType role) {
        return assignRole(role.name());
    }
    
    public RoleAssigner<ZookeeperTemplate> assignRoleServer() {
        return assignRole(ZookeeperRoleType.SERVER);
    }
 

    @Override
    protected void preServiceAddChecks(ClouderaApi api) {
        for (ServiceRoleHostInfo role: roles) {
            if (ZookeeperRoleType.SERVER.toString().equalsIgnoreCase(role.type)) {
                String hostId = (String)role.hostRef.get("hostId");
                if (manager==null) {
                    log.warn("No manager connected to zookeeper; cannot automatically set up dirs.");
                } else {
                    ClouderaCdhNode node = manager.findEntityForHostId(hostId);
                    if (node==null) {
                        log.warn("Manager "+manager+" does not know "+hostId+" for zookeeper; cannot automatically set up zookeeper dir.");
                    } else {
                        log.debug("Initializing zookeeper dir at "+hostId);
                        int resultCode = node.newScript("creating zookeeper dir at "+node+" for "+name).
                            body.append(
                                "sudo -u zookeeper mkdir -p /var/lib/zookeeper/version-2").
                            execute();
                        if (resultCode!=0) {
                            log.warn("Script failed initializing zookeeper dir at "+hostId+" (return code "+resultCode+"); zookeeper startup may fail.");
                        }
                    }
                }
            }
        }
    }

//    public static void main(String[] args) {
//        String SERVER = "ec2-107-22-7-107.compute-1.amazonaws.com",
//            H1 = "ip-10-114-39-149.ec2.internal",
//            H2 = "ip-10-144-18-90.ec2.internal",
//            H3 = "ip-10-60-154-21.ec2.internal";
//        
//        ClouderaRestCaller caller = ClouderaRestCaller.newInstance(SERVER, "admin", "admin");
//
//        System.out.println(new ZookeeperTemplate().
//                assignRoleServer().to(H1).
//            build(caller));
//    }


}
