package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.GroovyJavaMethods.elvis
import groovy.transform.InheritConstructors

import java.util.concurrent.TimeUnit

import org.jclouds.compute.domain.OsFamily
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.basic.BasicConfigurableEntityFactory
import brooklyn.entity.basic.ConfigurableEntityFactory
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.NamedParameter
import brooklyn.entity.basic.SoftwareProcessImpl
import brooklyn.entity.basic.lifecycle.ScriptHelper
import brooklyn.event.adapter.FunctionSensorAdapter
import brooklyn.location.MachineProvisioningLocation
import brooklyn.location.jclouds.templates.PortableTemplateBuilder


@InheritConstructors
public class ClouderaCdhNodeImpl extends SoftwareProcessImpl implements ClouderaCdhNode {

    private static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeImpl.class)
    
    public static ConfigurableEntityFactory<ClouderaCdhNodeImpl> newFactory() { 
        return new BasicConfigurableEntityFactory<ClouderaCdhNodeImpl>(ClouderaCdhNodeImpl.class);
    }
    
    public Class getDriverInterface() { return ClouderaCdhNodeSshDriver.class; }
    
    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        flags.templateBuilder = new PortableTemplateBuilder().
            osFamily(OsFamily.UBUNTU).osVersionMatches("12.04").
            os64Bit(true).
            minRam(2560);
        if(System.getProperty("securitygroup") != null) {
            flags.remove("inboundPorts");
            flags.put("securityGroups", System.getProperty("securitygroup"));
        }
        return flags;
    }

    // 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030
    protected Collection<Integer> getRequiredOpenPorts() {
        Set result = [22, 2181, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030];
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
    
    public String getManagedHostId() {
        def managedHosts = getConfig(MANAGER)?.getAttribute(ClouderaManagerNode.MANAGED_HOSTS);
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
        // TODO allow wildcards, or batch on server then copy down?
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
        for (role in ["mr","jvm"]) {
            try {
                ((ClouderaCdhNodeDriver)driver).machine.copyFrom(sshTries:1,
                    "/tmp/${role}metrics.log", targetDir+"/${role}metrics.log");
            } catch (Exception e) {
                //not serious, file probably doesn't exist
                log.debug("Unable to copy /tmp/${role}metrics.log from ${this} (file may not exist): "+e);
            }
        }
        log.debug("Copied ${i} metrics files from ${this}");
        return targetDir;
    }
    
}
