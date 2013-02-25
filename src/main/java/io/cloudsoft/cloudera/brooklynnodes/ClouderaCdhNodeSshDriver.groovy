package io.cloudsoft.cloudera.brooklynnodes;

import io.cloudsoft.cloudera.rest.ClouderaRestCaller;

import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.lifecycle.CommonCommands;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.util.ResourceUtils;

import com.google.common.base.Preconditions;

public class ClouderaCdhNodeSshDriver extends AbstractSoftwareProcessSshDriver implements ClouderaCdhNodeDriver {

    public static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeSshDriver.class);
    
    public ClouderaCdhNodeSshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

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
                updateTaskAndFailOnNonZeroResultCode().
                body.append(
                    CommonCommands.INSTALL_TAR,
                    "cd /tmp", 
                    "sudo mkdir /etc/cloudera-scm-agent/",
                    "sudo mv etc_cloudera-scm-agent_config.ini /etc/cloudera-scm-agent/config.ini",
                    "tar xzfv scm_prepare_node.tgz",
                    ).execute();
    }

    protected String getManagerHostname() {
        return DependentConfiguration.waitForTask(
            DependentConfiguration.attributeWhenReady(getManager(), WhirrClouderaManager.CLOUDERA_MANAGER_HOSTNAME),
            getEntity());
    }
    
    @Override
    public void customize() {
        log.info(""+this+" waiting for manager hostname and service-up from "+getManager()+" before installing SCM");
        DependentConfiguration.waitForTask(
            DependentConfiguration.attributeWhenReady(getManager(), WhirrClouderaManager.SERVICE_UP),
            getEntity());
        log.info(""+this+" got manager hostname as "+getManagerHostname());
        
        waitForManagerPingable();
        
        def script = newScript(CUSTOMIZING).
                body.append(
                    "cd /tmp/scm_prepare_node.X",
                    "sudo ./scm_prepare_node.sh"+
                        " -h"+getManagerHostname()+
                        " --packages /tmp/scm_prepare_node.X/packages.scm"+
                        " --always /tmp/scm_prepare_node.X/always_install.scm"+
                        " --x86_64 /tmp/scm_prepare_node.X/x86_64_packages.scm"
                    );
        int result = script.execute();
        if (result!=0) {
            log.warn(""+this+" first attempt to install SCM failed, exit code "+result+"; trying again");
            Thread.sleep(15*1000);
            script.updateTaskAndFailOnNonZeroResultCode().execute();
        }
        
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
        entity.setAttribute(ClouderaCdhNode.PRIVATE_IP, ipAddress);

        // but we do need to record the _on-box_ hostname as this is what it is knwon at at the manager        
        String hostname = getHostname();
        if (machine in JcloudsSshMachineLocation) {
            // returns on-box hostname
            hostname = ((JcloudsSshMachineLocation)machine).getNode().getHostname();
        }
        entity.setAttribute(ClouderaCdhNode.PRIVATE_HOSTNAME, hostname);
        
    }
    
    public void waitForManagerPingable() {
        def script = newScript("wait-for-manager").
            body.append(
                "ping -c 1 "+getManagerHostname());
        long maxEndTime = System.currentTimeMillis() + 120*1000;
        int i=0;
        while (true) {
            int result = script.execute();
            if (result==0) return;
            i++;
            log.debug("Not yet able to ping manager node "+getManagerHostname()+" from "+machine+" for "+entity+" (attempt "+i+")");
            if (System.currentTimeMillis()>maxEndTime)
                throw new IllegalStateException("Unable to ping manager node "+getManagerHostname()+" from "+machine+" for "+entity);
            Thread.sleep(1000);
        }
    }

    @Override
    public void launch() {
        // nothing needed here, services get launched separately
    }

}
