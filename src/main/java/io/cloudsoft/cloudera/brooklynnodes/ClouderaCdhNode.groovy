
package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.GroovyJavaMethods.elvis

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.Effector
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.MethodEffector
import brooklyn.entity.basic.NamedParameter
import brooklyn.entity.basic.SoftwareProcess
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.proxying.ImplementedBy
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.util.flags.SetFromFlag

@ImplementedBy(ClouderaCdhNodeImpl.class)
public interface ClouderaCdhNode extends SoftwareProcess {

    @SetFromFlag(value="manager", nullable=false)
    public static final BasicConfigKey<ClouderaManagerNode> MANAGER =
        [ClouderaManagerNode.class, "cloudera.cdh.node.manager", "Cloudera Manager entity"];

    public static final BasicAttributeSensor<String> PRIVATE_HOSTNAME =
        [String, "whirr.cm.cdh.node.internal.hostname", "Hostname of this node as known on internal/private subnets"]
    
    public static final BasicAttributeSensor<String> PRIVATE_IP =
        [String, "whirr.cm.cdh.node.internal.ip", "IP of this node as known on internal/private subnets"]
    
    public static final BasicAttributeSensor<String> CDH_HOST_ID =
        [String, "whirr.cm.cdh.node.id", "ID of host as presented to CM (usually internal hostname)"]

    public static final Effector<String> COLLECT_METRICS = new MethodEffector<String>(this.&collectMetrics);
     
    /**
     * Start the entity in the given collection of locations.
     */
    @Description("Collect metrics files from this host and save to a file on this machine, as a subdir of the given dir, returning the name of that subdir")
    public String collectMetrics(@NamedParameter("targetDir") String targetDir);

    public ScriptHelper newScript(String summary);
    
    public String getManagedHostId();
    
}
