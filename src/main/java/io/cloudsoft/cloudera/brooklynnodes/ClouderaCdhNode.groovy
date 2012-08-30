
package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.GroovyJavaMethods.elvis
import groovy.transform.InheritConstructors

import java.util.concurrent.TimeUnit

import org.jclouds.compute.domain.OsFamily
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.Effector
import brooklyn.entity.basic.BasicConfigurableEntityFactory
import brooklyn.entity.basic.ConfigurableEntityFactory
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.MethodEffector
import brooklyn.entity.basic.NamedParameter
import brooklyn.entity.basic.SoftwareProcessEntity
import brooklyn.entity.basic.lifecycle.ScriptHelper
import brooklyn.entity.basic.lifecycle.StartStopDriver
import brooklyn.event.adapter.FunctionSensorAdapter
import brooklyn.event.basic.BasicAttributeSensor
import brooklyn.event.basic.BasicConfigKey
import brooklyn.location.MachineProvisioningLocation
import brooklyn.location.basic.SshMachineLocation
import brooklyn.location.basic.jclouds.templates.PortableTemplateBuilder
import brooklyn.util.flags.SetFromFlag

@InheritConstructors
public class ClouderaCdhNode extends SoftwareProcessEntity {

    private static final Logger log = LoggerFactory.getLogger(ClouderaCdhNode.class)
    
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

    public static final Effector<String> COLLECT_METRICS = new MethodEffector<String>(this.&collectMetrics);
        
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

    // 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030
    protected Collection<Integer> getRequiredOpenPorts() {
        Set result = [22, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030];
        result.addAll(super.getRequiredOpenPorts());
        return result;
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

    /**
     * Start the entity in the given collection of locations.
     */
    @Description("Collect metrics files from this host and save to a file on this machine, as a subdir of the given dir, returning the name of that subdir")
    public String collectMetrics(@NamedParameter("targetDir") String targetDir) {
        targetDir = targetDir + "/" + getAttribute(CDH_HOST_ID);
        new File(targetDir).mkdir();
        // TODO allow wildcards
        int i=0;
        for (role in ["datanode","namenode","master","regionserver"]) {
            try {
                ((ClouderaCdhNodeDriver)driver).machine.copyFrom(sshTries:1,
                    "/tmp/${role}-metrics.out", targetDir+"/${role}-metrics.out");
            } catch (Exception e) {
                //not serious, file probably doesn't exist
                log.debug("Unable to copy /tmp/${role}-metrics.out from ${this} (file may not exist): "+e);
            }
        }
        log.debug("Copied ${i} metrics files from ${this}");
        return targetDir;
    }
    
}
