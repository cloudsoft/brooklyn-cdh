package io.cloudsoft.cloudera.brooklynnodes;

import static java.lang.String.format
import groovy.transform.InheritConstructors
import io.cloudsoft.cloudera.rest.ClouderaRestCaller

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.basic.Lifecycle
import brooklyn.entity.basic.lifecycle.CommonCommands
import brooklyn.entity.basic.lifecycle.StartStopSshDriver
import brooklyn.event.basic.DependentConfiguration
import brooklyn.location.basic.jclouds.JcloudsLocation.JcloudsSshMachineLocation
import brooklyn.util.ResourceUtils

import com.google.common.base.Preconditions

@InheritConstructors
public class ClouderaCdhNodeDriver extends StartStopSshDriver {

    public static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeDriver.class);
    
    protected WhirrClouderaManager getManager() {
        return Preconditions.checkNotNull(getEntity().getConfig(ClouderaCdhNode.MANAGER));
    }
    
    @Override
    public boolean isRunning() {
        return ((ClouderaCdhNode)entity).getManagedHostId()!=null;
    }

    @Override
    public void stop() {
        // just wait for machine to be killed
    }

    @Override
    public void install() {
        machine.copyTo(new ResourceUtils(entity).getResourceFromUrl("classpath://scm_prepare_node.tgz"), "/tmp/scm_prepare_node.tgz");
        machine.copyTo(new ResourceUtils(entity).getResourceFromUrl("classpath://etc_cloudera-scm-agent_config.ini"), "/tmp/etc_cloudera-scm-agent_config.ini");

        newScript(INSTALLING).
                failOnNonZeroResultCode().
                body.append(
                    CommonCommands.INSTALL_TAR,
                    "cd /tmp", 
                    "sudo mkdir /etc/cloudera-scm-agent/",
                    "sudo mv etc_cloudera-scm-agent_config.ini /etc/cloudera-scm-agent/config.ini",
                    "tar xzfv scm_prepare_node.tgz",
                    "cd scm_prepare_node.X"
                    ).execute();
    }

    protected String getManagerHostname() {
        return DependentConfiguration.waitForTask(
            DependentConfiguration.attributeWhenReady(getManager(), WhirrClouderaManager.CLOUDERA_MANAGER_HOSTNAME),
            getEntity());
    }
    
    @Override
    public void customize() {
        log.info(""+this+" waiting for manager hostname from "+getManager()+" before customising (manager must be pingable)");
        log.info(""+this+" got manager hostname as "+getManagerHostname());
        
        newScript(CUSTOMIZING).
                failOnNonZeroResultCode().
                body.append(
                    CommonCommands.INSTALL_TAR,
                    "cd /tmp/scm_prepare_node.X",
                    "sudo ./scm_prepare_node.sh"+
                        " -h"+getManagerHostname()+
                        " --packages /tmp/scm_prepare_node.X/packages.scm"+
                        " --always /tmp/scm_prepare_node.X/always_install.scm"+
                        " --x86_64 /tmp/scm_prepare_node.X/x86_64_packages.scm"
                    ).execute();
                
        DependentConfiguration.waitForTask(
                    DependentConfiguration.attributeWhenReady(getManager(), WhirrClouderaManager.SERVICE_UP),
                    getEntity());

        def caller = new ClouderaRestCaller(server: getManagerHostname(), authName:"admin", authPass:"admin");

        // this entity seems to be picked up automatically at manager when agent starts on CDH node, no need to REST add call
        String ipAddress = null;
        if (machine in JcloudsSshMachineLocation) {
            def addrs = ((JcloudsSshMachineLocation)machine).getNode().getPrivateAddresses();
            if (addrs) {
                ipAddress = addrs.iterator().next();
                log.info("IP address (private) of "+machine+" for "+entity+" detected as "+ipAddress);
            } else {
                addrs = ((JcloudsSshMachineLocation)machine).getNode().getPublicAddresses();
                if (addrs) {
                    log.info("IP address (public) of "+machine+" for "+entity+" detected as "+ipAddress);
                    ipAddress = addrs.iterator().next();
                }
            }
        }
        if (ipAddress==null) {
            ipAddress = InetAddress.getByName(hostname)?.getHostAddress();
            log.info("IP address (hostname) of "+machine+" for "+entity+" detected as "+ipAddress);
        }
        entity.setAttribute(ClouderaCdhNode.PRIVATE_IP, hostname);

        // but we do need to record the _on-box_ hostname as this is what it is knwon at at the manager        
        String hostname = getHostname();
        if (machine in JcloudsSshMachineLocation) {
            // returns on-box hostname
            hostname = ((JcloudsSshMachineLocation)machine).getNode().getHostname();
        }
        entity.setAttribute(ClouderaCdhNode.PRIVATE_HOSTNAME, hostname);
        
    }

    @Override
    public void launch() {
        // nothing needed here, services get launched separately
    }

}
