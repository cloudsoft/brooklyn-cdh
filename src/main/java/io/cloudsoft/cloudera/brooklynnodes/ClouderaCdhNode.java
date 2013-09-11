
package io.cloudsoft.cloudera.brooklynnodes;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.event.basic.BasicConfigKey;
import brooklyn.util.flags.SetFromFlag;

@ImplementedBy(ClouderaCdhNodeImpl.class)
public interface ClouderaCdhNode extends SoftwareProcess {

    @SetFromFlag(value="manager", nullable=false)
    public static final ConfigKey<ClouderaManagerNode> MANAGER = new BasicConfigKey<ClouderaManagerNode>(
    		ClouderaManagerNode.class, "cloudera.cdh.node.manager", "Cloudera Manager entity");

    public static final AttributeSensor<String> PRIVATE_HOSTNAME = new BasicAttributeSensor<String>(
        	String.class, "brooklyn.cdh.node.internal.hostname", "Hostname of this node as known on internal/private subnets");
    
    public static final AttributeSensor<String> PRIVATE_IP = new BasicAttributeSensor<String>(
        	String.class, "brooklyn.cdh.node.internal.ip", "IP of this node as known on internal/private subnets");
    
    public static final AttributeSensor<String> CDH_HOST_ID = new BasicAttributeSensor<String>(
        	String.class, "brooklyn.cdh.node.id", "ID of host as presented to CM (usually internal hostname)");
    
    public static final BasicConfigKey<String> APT_PROXY = 
            new BasicConfigKey<String>(String.class, "cloudera.cdh.node.apt.proxy.url", "URL of apt proxy");

    public static final Effector<String> COLLECT_METRICS = new MethodEffector<String>(ClouderaCdhNode.class, "collectMetrics");
    
    public static final AttributeSensor<String> LOCAL_HOSTNAME = ClouderaManagerNode.LOCAL_HOSTNAME;    
     
    /**
     * Start the entity in the given collection of locations.
     */
    @brooklyn.entity.annotation.Effector(description="Collect metrics files from this host and save to a file on this machine, as a subdir of the given dir, returning the name of that subdir")
    public String collectMetrics(@EffectorParam(name="targetDir") String targetDir);

    public ScriptHelper newScript(String summary);
    
    public String getManagedHostId();
    
}
