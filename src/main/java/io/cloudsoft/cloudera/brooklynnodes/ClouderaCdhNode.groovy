package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.GroovyJavaMethods.elvis;

import java.util.concurrent.TimeUnit;

import groovy.transform.InheritConstructors

import org.jclouds.compute.domain.OsFamily

import brooklyn.entity.basic.BasicConfigurableEntityFactory
import brooklyn.entity.basic.ConfigurableEntityFactory
import brooklyn.entity.basic.SoftwareProcessEntity
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.basic.lifecycle.StartStopDriver
import brooklyn.event.adapter.FunctionSensorAdapter;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.event.basic.BasicConfigKey
import brooklyn.location.MachineProvisioningLocation
import brooklyn.location.basic.SshMachineLocation
import brooklyn.location.basic.jclouds.templates.PortableTemplateBuilder
import brooklyn.util.flags.SetFromFlag

@InheritConstructors
public class ClouderaCdhNode extends SoftwareProcessEntity {

    public static ConfigurableEntityFactory<ClouderaCdhNode> newFactory() { 
        return new BasicConfigurableEntityFactory<ClouderaCdhNode>(ClouderaCdhNode.class);
    }
    
    @SetFromFlag(value="manager", nullable=false)
    public static final BasicConfigKey<WhirrClouderaManager> MANAGER =
        [WhirrClouderaManager.class, "cloudera.cdh.node.manager", "Cloudera Manager entity"];

    public static final BasicAttributeSensor<String> PRIVATE_HOSTNAME =
        [String, "whirr.cm.cdh.node.internal.hostname", "Hostname of this node as known on internal/private subnets"]
    
    public static final BasicAttributeSensor<String> PRIVATE_IP =
        [String, "whirr.cm.cdh.node.internal.ip", "IP of this node as known on internal/private subnets"]
    
    public static final BasicAttributeSensor<String> CDH_HOST_ID =
        [String, "whirr.cm.cdh.node.id", "ID of host as presented to CM (usually internal hostname)"]

    @Override
    protected StartStopDriver newDriver(SshMachineLocation loc) {
        return new ClouderaCdhNodeDriver(this, loc);
    }

    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        flags.templateBuilder = new PortableTemplateBuilder().
            osFamily(OsFamily.UBUNTU).osVersionMatches("12.04").
            os64Bit(true).
            minRam(2560);
        return flags;
    }

    public void connectSensors() {
        super.connectSensors();
        
        FunctionSensorAdapter fnSensorAdaptor = sensorRegistry.register(new FunctionSensorAdapter({}, period: 30*TimeUnit.SECONDS));
        def mgdh = fnSensorAdaptor.then { getManagedHostId() };
        mgdh.poll(CDH_HOST_ID);
        mgdh.poll(SERVICE_UP, { it!=null });
    }
    
    protected String getManagedHostId() {
        def managedHosts = getConfig(MANAGER)?.getAttribute(WhirrClouderaManager.MANAGED_HOSTS);
        if (!managedHosts) return null;
        String hostname = getAttribute(HOSTNAME);
        if (hostname && managedHosts.contains(hostname)) return hostname;
        String privateHostname = getAttribute(PRIVATE_HOSTNAME);
        if (privateHostname) {
            // manager might view it as ip-10-1-1-1.ec2.internal whereas node knows itself as just ip-10-1-1-1
            // TODO better might be to compare private IP addresses of this node with IP of managed nodes at CM  
            def pm = managedHosts.find { it.startsWith(privateHostname) }
            if (pm) return pm;
        }
        return null;
    }

    public ScriptHelper newScript(String summary) {
        return new ScriptHelper(driver, summary);
    }
}
