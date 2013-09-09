package io.cloudsoft.cloudera.brooklynnodes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.effector.EffectorTasks;
import brooklyn.entity.software.SshEffectorTasks;
import brooklyn.entity.software.SshEffectorTasks.SshEffectorTaskFactory;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.util.ResourceUtils;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.task.DynamicTasks;
import brooklyn.util.task.system.ProcessTaskWrapper;
import brooklyn.util.text.Strings;
import brooklyn.util.time.Duration;
import brooklyn.util.time.Time;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

public class ClouderaCdhNodeSshDriver extends AbstractSoftwareProcessSshDriver implements ClouderaCdhNodeDriver {

    public static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeSshDriver.class);
    
    public ClouderaCdhNodeSshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    protected ClouderaManagerNode getManager() {
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
        entity.setAttribute(ClouderaCdhNode.LOCAL_HOSTNAME, execHostname());

        // TODO move to io/cloudsoft/cloudera inside src/main/resources
        getMachine().copyTo(new ResourceUtils(entity).getResourceFromUrl("classpath://scm_prepare_node.tgz"), "/tmp/scm_prepare_node.tgz");
        getMachine().copyTo(new ResourceUtils(entity).getResourceFromUrl("classpath://etc_cloudera-scm-agent_config.ini"), "/tmp/etc_cloudera-scm-agent_config.ini");
        
        String aptProxyUrl = getLocation().getConfig(ClouderaCdhNode.APT_PROXY);
        if(Strings.isNonEmpty(aptProxyUrl)) {
            InputStream proxy = generatePackageManagerProxyFile(aptProxyUrl);
            getMachine().copyTo(proxy, "/tmp/02proxy");
            newScript(INSTALLING+":setAptProxy")
                .body.append(BashCommands.sudo("mv /tmp/02proxy /etc/apt/apt.conf.d/02proxy"))
                .execute();
        }
        
        List<String> commands = Lists.newArrayList();
        commands.add(BashCommands.INSTALL_TAR);
        commands.add("cd /tmp");
        commands.add("sudo mkdir /etc/cloudera-scm-agent/");
        commands.add("sudo mv etc_cloudera-scm-agent_config.ini /etc/cloudera-scm-agent/config.ini");
        commands.add("tar xzfv scm_prepare_node.tgz");
        
        newScript(INSTALLING).
                updateTaskAndFailOnNonZeroResultCode().
                body.append(commands).execute();
    }

    private InputStream generatePackageManagerProxyFile(String aptProxyUrl) {
        InputStream proxy = Preconditions.checkNotNull(new ResourceUtils(this).getResourceFromUrl("02proxy"), "cannot find 02proxy");
        try {
            String template = CharStreams.toString(new InputStreamReader(proxy));
            String aptProxy = template.replaceFirst("ip-address", aptProxyUrl);
            log.debug("PackageManagerProxy set up at: " + aptProxy);
            return new ByteArrayInputStream(aptProxy.getBytes("UTF-8"));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
    
    protected String getManagerHostname() {
        try {
            return DependentConfiguration.waitForTask(
                DependentConfiguration.attributeWhenReady(getManager(), ClouderaManagerNode.CLOUDERA_MANAGER_HOSTNAME),
                getEntity());
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @Override
    public void customize() {       
        log.info(""+this+" waiting for manager hostname and service-up from "+getManager()+" before installing SCM");
        try {
            DependentConfiguration.waitForTask(
                DependentConfiguration.attributeWhenReady(getManager(), SoftwareProcess.SERVICE_UP),
                getEntity());
        } catch (InterruptedException e1) {
            throw Exceptions.propagate(e1);
        }
        log.info(""+this+" got manager hostname as "+getManagerHostname());
        
        waitForManagerPingable();
        waitForPingable("client-visible-from-manager", getHostname(), getManager(), Duration.TWO_MINUTES);
        
        ScriptHelper script = newScript(CUSTOMIZING).
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
            Time.sleep(15*1000);
            script.updateTaskAndFailOnNonZeroResultCode().execute();
        }

        discoverSubnetAddressInfo();
    }

    private void discoverSubnetAddressInfo() {
        // this entity seems to be picked up automatically at manager when agent starts on CDH node, no need to REST add call
        String ipAddress = null;
        if (getMachine() instanceof JcloudsSshMachineLocation) {
            Set<String> addrs = ((JcloudsSshMachineLocation)getMachine()).getNode().getPrivateAddresses();
            if (!addrs.isEmpty()) {
                ipAddress = addrs.iterator().next();
                log.info("IP address (private) of "+getMachine()+" for "+entity+" detected as "+ipAddress);
            } else {
                addrs = ((JcloudsSshMachineLocation)getMachine()).getNode().getPublicAddresses();
                if (!addrs.isEmpty()) {
                    log.info("IP address (public) of "+getMachine()+" for "+entity+" detected as "+ipAddress);
                    ipAddress = addrs.iterator().next();
                }
            }
        }
        if (ipAddress==null) {
            InetAddress ipAddressIA;
            try {
                ipAddressIA = InetAddress.getByName(getHostname());
                if (ipAddressIA!=null) ipAddress = ipAddressIA.getHostAddress();
                log.info("IP address (hostname) of "+getMachine()+" for "+entity+" detected as "+ipAddress);
            } catch (UnknownHostException e) {
                throw new IllegalStateException("Cannor resolve IP address for "+getMachine()+"/"+getHostname()+": "+e, e);
            }
        }
        entity.setAttribute(ClouderaCdhNode.PRIVATE_IP, ipAddress);

        // but we do need to record the _on-box_ hostname as this is what it is knwon at at the manager        
        String hostname = getHostname();
        log.info("hostname of "+getMachine()+" for "+entity+" is "+ hostname + "(ipaddress= " + ipAddress + ")");
        if (getMachine() instanceof JcloudsSshMachineLocation) {
            // returns on-box hostname
            hostname = ((JcloudsSshMachineLocation)getMachine()).getNode().getHostname();
        }
        entity.setAttribute(ClouderaCdhNode.PRIVATE_HOSTNAME, hostname);
    }
    
    public void waitForPingable(String description, String targetHostname, Entity fromEntity, Duration timeout) {
        SshMachineLocation sourceMachine = EffectorTasks.getSshMachine(fromEntity);
        SshEffectorTaskFactory<Integer> sshF = SshEffectorTasks.ssh("ping -c 1 "+targetHostname)
            .allowingNonZeroExitCode()
            .machine(sourceMachine)
            .summary(description);
        long endTime = System.currentTimeMillis() + timeout.toMilliseconds();
        int i=0;
        boolean warned = false;
        while (true) {
            i++;
            ProcessTaskWrapper<Integer> task = DynamicTasks.queue(sshF).block();
            if (task.get()==0) {
                if (task.getStdout().indexOf("ubuntu.localdomain")>0) {
                    String msg = "Pinging "+targetHostname+" ("+description+") from "+fromEntity+" includes phrase 'ubuntu.localdomain'";
                    if (System.currentTimeMillis()>endTime) {
                        log.warn(msg+"; timeout encountered, so continuing");
                        return;
                    }
                    if (warned==false) {
                        log.warn(msg+"; will keep trying (future failures logged at debug)");
                        warned = true;
                    } else {
                        log.debug(msg+"; will keep trying (future failures logged at debug)");                        
                    }
                } else {
                    if (warned) {
                        log.info("Pinging "+targetHostname+" ("+description+") from "+fromEntity+" no longer includes phrase 'ubuntu.localdomain'");
                    }
                    return;
                }
            }
            log.debug("Not yet able to ping node "+targetHostname+" ("+description+") from "+sourceMachine+" (attempt "+i+")");
            if (System.currentTimeMillis()>endTime)
                throw new IllegalStateException("Timeout: pinging "+targetHostname+" from "+sourceMachine);
            Time.sleep(Duration.ONE_SECOND);
        }
    }
    
    public void waitForManagerPingable() {
        waitForPingable("wait-for-manager", getManagerHostname(), getEntity(), Duration.TWO_MINUTES);
    }

    @Override
    public void launch() {
        // nothing needed here, services get launched separately
    }
    
    @Override
    public String toString() {
        return "SshDriver["+entity+"]";
    }
    
   // TODO Move up to a super-type
   private String execHostname() {
      if (log.isTraceEnabled())
         log.trace("Retrieve `hostname` via ssh for {}", this);
      ProcessTaskWrapper<Integer> cmd = DynamicTasks.queue(SshEffectorTasks.ssh("echo hostname=`hostname`").summary("getHostname")).block();
      
      for (String line : cmd.getStdout().split("\n")) {
         if (line.contains("hostname=") && !line.contains("`hostname`")) {
            return line.substring(line.indexOf("hostname=") + "hostname=".length());
         }
      }
      log.info("No hostname found for {} (got {}; {})", this, cmd.getStdout(), cmd.getStderr());
      return null;
   }

}
