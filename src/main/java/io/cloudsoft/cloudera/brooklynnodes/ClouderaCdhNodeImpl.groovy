package io.cloudsoft.cloudera.brooklynnodes

import static brooklyn.util.GroovyJavaMethods.elvis;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode.log;
import groovy.transform.InheritConstructors;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApiMetadata;
import org.jclouds.openstack.nova.v2_0.config.NovaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import brooklyn.entity.basic.BasicConfigurableEntityFactory;
import brooklyn.entity.basic.ConfigurableEntityFactory;
import brooklyn.entity.basic.Description;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.NamedParameter;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.cloud.CloudLocationConfig;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import brooklyn.util.time.Time;

import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

@InheritConstructors
public class ClouderaCdhNodeImpl extends SoftwareProcessImpl implements ClouderaCdhNode {

	FunctionFeed feed;
	
    @Override
    public void waitForEntityStart() {
        if (log.isDebugEnabled()) log.debug("waiting to ensure {} doesn't abort prematurely", this);
        long startTime = System.currentTimeMillis();
        long waitTime = startTime + 200000; // FIXME magic number; should be config key with default value?
        boolean isRunningResult = false;
        while (!isRunningResult && System.currentTimeMillis() < waitTime) {
            Time.sleep(1000); // FIXME magic number; should be config key with default value?
            try {
                isRunningResult = driver.isRunning();
            } catch (Exception  e) {
                setAttribute(SERVICE_STATE, Lifecycle.ON_FIRE);
                // provide extra context info, as we're seeing this happen in strange circumstances
                if (driver==null) throw new IllegalStateException(this+" concurrent start and shutdown detected");
                throw new IllegalStateException("Error detecting whether "+this+" is running: "+e, e);
            }
            if (log.isDebugEnabled()) log.debug("checked {}, is running returned: {}", this, isRunningResult);
        }
        if (!isRunningResult) {
            String msg = "Software process entity "+this+" did not appear to start within "+
                    Time.makeTimeString(System.currentTimeMillis()-startTime)+
                    "; setting state to indicate problem and throwing; consult logs for more details";
            log.warn(msg);
            setAttribute(SERVICE_STATE, Lifecycle.ON_FIRE);
            throw new IllegalStateException(msg);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeImpl.class);
    
    public static ConfigurableEntityFactory<ClouderaCdhNodeImpl> newFactory() { 
        return new BasicConfigurableEntityFactory<ClouderaCdhNodeImpl>(ClouderaCdhNodeImpl.class);
    }
    
    public Class getDriverInterface() { return ClouderaCdhNodeSshDriver.class; }
    
    protected Map<String, Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        PortableTemplateBuilder portableTemplateBuilder = new PortableTemplateBuilder();
        if (isJcloudsLocation(location, "aws-ec2")) {
            portableTemplateBuilder.osFamily(OsFamily.UBUNTU).osVersionMatches("12.04").os64Bit(true).minRam(2500);
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(), portableTemplateBuilder);
        } else if (isJcloudsLocation(location, "google-compute-engine")) {
            flags.putAll(GoogleComputeEngineApiMetadata.defaultProperties());
            flags.put("groupId", "brooklyn-cdh");
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
            portableTemplateBuilder.osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true)
            .locationId("us-central1-a").minRam(2560));
        } else if (isJcloudsLocation(location, "openstack-nova")) {
            String imageId = location.getConfig(JcloudsLocationConfig.IMAGE_ID);
            if(imageId != null) {
                portableTemplateBuilder.imageId(imageId);
            }
            String hardwareId = location.getConfig(JcloudsLocationConfig.HARDWARE_ID);
            if(hardwareId != null) {
                portableTemplateBuilder.hardwareId(hardwareId);
            }
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(), portableTemplateBuilder);
        } else if (isJcloudsLocation(location, "rackspace-cloudservers-uk") || isJcloudsLocation(location, "cloudservers-uk")) {
            // securityGroups are not supported
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
            portableTemplateBuilder.osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true)
            .minRam(2560));
        } else if (isJcloudsLocation(location, "bluelock-vcloud-zone01")) {
            System.setProperty("jclouds.vcloud.timeout.task-complete", 600 * 1000 + "");
            // this is a constraint for dns name on vcloud (3-15 characters)
            flags.put("groupId", "brooklyn");
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
        //    portableTemplateBuilder.osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true));
            portableTemplateBuilder.imageId("https://zone01.bluelock.com/api/v1.0/vAppTemplate/vappTemplate-e0717fc0-0b7f-41f7-a275-3e03881d99db"));
        }
        return flags;
        }
    
    private boolean isJcloudsLocation(MachineProvisioningLocation location, String providerName) {
        return location instanceof JcloudsLocation && ((JcloudsLocation) location).getProvider().equals(providerName);
    }

    // 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030
    protected Collection<Integer> getRequiredOpenPorts() {
        Set result = [22, 2181, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030];
        result.addAll(super.getRequiredOpenPorts());
        return result;
    }
    
    public void connectSensors() {
        super.connectSensors();
		
        feed = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_UP)
                .period(30, TimeUnit.SECONDS)
                .callable(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        try { 
                            return getManagedHostId() != null; 
                        } catch (Exception e) {
                            log.error(e); 
                            return false; 
                        }
                    }
                })
                .onError(Functions.constant(false))
                )
                .poll(new FunctionPollConfig<List,List>(CDH_HOST_ID)
                .period(30, TimeUnit.SECONDS)
                .callable(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return getManagedHostId();
                    }
                })
                .onError(Functions.constant(false))
                )
                .build();
    }
    
    @Override
    protected void disconnectSensors() {
    	super.disconnectSensors();
    	if (feed != null) feed.stop();
    }
    
    public String getManagedHostId() {
        def managedHosts = getConfig(MANAGER)?.getAttribute(ClouderaManagerNode.MANAGED_HOSTS);
        if (!managedHosts) return null;
        
        String localHostname = getAttribute(LOCAL_HOSTNAME);
        if (localHostname && managedHosts.contains(localHostname)) {
            log.debug("ManagedHostId is local hostname: "+ localHostname);
            return localHostname;
        }
        
        String hostname = getAttribute(HOSTNAME);
        if (hostname && managedHosts.contains(hostname)) {
            log.debug("ManagedHostId is hostname: "+ hostname);
            return hostname;
        }
        
        String privateHostname = getAttribute(PRIVATE_HOSTNAME);
        if (privateHostname) {
            // manager might view it as ip-10-1-1-1.ec2.internal whereas node knows itself as just ip-10-1-1-1
            // TODO better might be to compare private IP addresses of this node with IP of managed nodes at CM  
            def pm = managedHosts.find { it.startsWith(privateHostname) }
            
            if (pm) {
                log.debug("ManagedHostId is private hostname: "+ pm);
                return pm;
            } else {
                log.debug("ManagedHostId (using private-hostname, not in managed-hosts): "+ privateHostname);
                return privateHostname;
            }
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
