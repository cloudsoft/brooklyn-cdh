package io.cloudsoft.cloudera.brooklynnodes

import static brooklyn.util.GroovyJavaMethods.elvis
import static com.google.common.base.Preconditions.checkNotNull;
import static io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode.log;
import groovy.transform.InheritConstructors

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

import org.jclouds.compute.domain.OsFamily
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApiMetadata;
import org.jclouds.openstack.nova.v2_0.config.NovaProperties;
import org.slf4j.Logger
import org.slf4j.LoggerFactory


import brooklyn.entity.basic.BasicConfigurableEntityFactory
import brooklyn.entity.basic.ConfigurableEntityFactory
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.NamedParameter
import brooklyn.entity.basic.SoftwareProcessImpl
import brooklyn.entity.basic.lifecycle.ScriptHelper
import brooklyn.event.feed.function.FunctionFeed
import brooklyn.event.feed.function.FunctionPollConfig
import brooklyn.location.MachineProvisioningLocation
import brooklyn.location.cloud.CloudLocationConfig;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder

import com.google.common.base.Charsets;
import com.google.common.base.Functions
import com.google.common.base.Throwables;
import com.google.common.io.Files;

@InheritConstructors
public class ClouderaCdhNodeImpl extends SoftwareProcessImpl implements ClouderaCdhNode {

    private static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeImpl.class)
    
    public static ConfigurableEntityFactory<ClouderaCdhNodeImpl> newFactory() { 
        return new BasicConfigurableEntityFactory<ClouderaCdhNodeImpl>(ClouderaCdhNodeImpl.class);
    }
    
    public Class getDriverInterface() { return ClouderaCdhNodeSshDriver.class; }
    
    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        if (isJcloudsLocation(location, "google-compute-engine")) {
            flags.putAll(GoogleComputeEngineApiMetadata.defaultProperties());
            flags.put("groupId", "brooklyn-cdh");
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
                    new PortableTemplateBuilder().osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true)
                            .locationId("us-central1-a").minRam(2560));
        } else if (isJcloudsLocation(location, "openstack-nova")) {
            flags.put(CloudLocationConfig.CLOUD_ENDPOINT, "https://cloudfirst.demos.ibm.com/keystone/v2.0");
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
                    new PortableTemplateBuilder().imageId("RegionOne/eeced716-bb37-4f3b-a3d6-977e17f20b21")
                    .hardwareId("RegionOne/9"));
            flags.put(NovaProperties.AUTO_ALLOCATE_FLOATING_IPS, "false");
            flags.put(NovaProperties.AUTO_GENERATE_KEYPAIRS, "false");
            Object securityGroups = 
                    checkNotNull(location.getConfig(JcloudsLocationConfig.SECURITY_GROUPS), "securityGroups must be declared");
            flags.put(JcloudsLocationConfig.SECURITY_GROUPS.getName(), securityGroups);
            String keyPair = checkNotNull(location.getConfig(JcloudsLocationConfig.KEY_PAIR), "keypair must be declared");
            flags.put(JcloudsLocationConfig.KEY_PAIR.getName(), keyPair);
            String loginUserPrivateKeyFileName = 
                    checkNotNull(location.getConfig(JcloudsLocationConfig.LOGIN_USER_PRIVATE_KEY_FILE), "login user private key must be declared");
            flags.put(JcloudsLocationConfig.LOGIN_USER_PRIVATE_KEY_FILE.getName(), loginUserPrivateKeyFileName);
        } else if (isJcloudsLocation(location, "rackspace-cloudservers-uk") ||
                isJcloudsLocation(location, "cloudservers-uk")) {
            flags.put(JcloudsLocationConfig.TEMPLATE_BUILDER.getName(),
                    new PortableTemplateBuilder().osFamily(OsFamily.CENTOS).osVersionMatches("6").os64Bit(true)
                    .minRam(2560));
        } else {
            flags.put(NovaProperties.AUTO_ALLOCATE_FLOATING_IPS.getName(),
                    System.getProperty(NovaProperties.AUTO_ALLOCATE_FLOATING_IPS.getName(), "false"));
            flags.put(NovaProperties.AUTO_GENERATE_KEYPAIRS.getName(),
                    System.getProperty(NovaProperties.AUTO_GENERATE_KEYPAIRS.getName(), "true"));
            flags.put(JcloudsLocationConfig.SECURITY_GROUPS.getName(),
                    System.getProperty("jclouds.securityGroups", "universal"));
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
        FunctionFeed feed = FunctionFeed.builder()
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
