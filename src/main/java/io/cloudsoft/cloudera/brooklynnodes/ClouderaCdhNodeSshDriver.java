package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.ssh.CommonCommands.INSTALL_WGET;
import static brooklyn.util.ssh.CommonCommands.alternatives;
import static brooklyn.util.ssh.CommonCommands.exists;
import static brooklyn.util.ssh.CommonCommands.sudo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import joptsimple.internal.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.util.ResourceUtils;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.ssh.CommonCommands;
import brooklyn.util.time.Time;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
        
        List<String> commands = Lists.newArrayList();
        commands.add(CommonCommands.INSTALL_TAR);
        commands.add("cd /tmp");
        commands.add("sudo mkdir /etc/cloudera-scm-agent/");
        commands.add("sudo mv etc_cloudera-scm-agent_config.ini /etc/cloudera-scm-agent/config.ini");
        commands.add("tar xzfv scm_prepare_node.tgz");
        newScript(INSTALLING).
        updateTaskAndFailOnNonZeroResultCode().
        body.append(commands).execute();
        
        // OS depending
        commands.clear();
        String packageManagerMirrorUrl = getLocation().getConfig(ClouderaCdhNode.PACKAGE_MANAGER_MIRROR_URL);
        if (!Strings.isNullOrEmpty(packageManagerMirrorUrl)) {
           String failure = String.format("(echo \"WARNING: no known/successful way found to install package manager mirror\")");
           commands.add(installPackageManagerMirrorOnDebianAndUbuntu(packageManagerMirrorUrl));
           commands.add(installPackageManagerMirrorOnRedHatAndDerivatives(packageManagerMirrorUrl));
           newScript(INSTALLING + ":setPackageManagerMirror").body.append(CommonCommands.alternatives(commands, failure)).execute();
        }
        /*
        if(!Strings.isNullOrEmpty(packageManagerMirrorUrl)) {
           if(isPackageManagerAvailable("apt-get")) {
            InputStream proxy = generateAptMirrorFile(packageManagerMirrorUrl);
            getMachine().copyTo(proxy, "/tmp/02proxy");
            newScript(INSTALLING+":setAptProxy")
                .body.append(CommonCommands.sudo("mv /tmp/02proxy /etc/apt/apt.conf.d/02proxy"))
                .execute();
           } else if (isPackageManagerAvailable("yum")) {
              commands.add("cd scm_prepare_node.X/repos/");
           }
        }
         */
    }


   private String installPackageManagerMirrorOnDebianAndUbuntu(String packageManagerMirrorUrl) {
      InputStream proxy = generateAptMirrorFile(packageManagerMirrorUrl);
      getMachine().copyTo(proxy, "/tmp/02proxy");
      return exists("apt-get", CommonCommands.sudo("mv /tmp/02proxy /etc/apt/apt.conf.d/02proxy"));
   }

   private String installPackageManagerMirrorOnRedHatAndDerivatives(String packageManagerMirrorUrl) {
      String mirrorHostname = Iterables.get(Splitter.on("//").split(packageManagerMirrorUrl), 1);
      return exists("yum", "unset OS_MAJOR; OS_MAJOR=`head -1 /etc/issue | awk '{ print $3 }' | cut -d'.' -f1`",
            "cd /tmp/scm_prepare_node.X/repos/rhel$OS_MAJOR/",
            String.format("sed -i 's=archive.cloudera.com=%s=g' ./cloudera-cdh4.repo", mirrorHostname),
            String.format("sed -i 's=archive.cloudera.com=%s=g' ./cloudera-manager.repo", mirrorHostname),
            sudo("yum clean all"));
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
    
    public void waitForManagerPingable() {
        ScriptHelper script = newScript("wait-for-manager").
            body.append(
                "ping -c 1 "+getManagerHostname());
        long maxEndTime = System.currentTimeMillis() + 120*1000;
        int i=0;
        while (true) {
            int result = script.execute();
            if (result==0) return;
            i++;
            log.debug("Not yet able to ping manager node "+getManagerHostname()+" from "+getMachine()+" for "+entity+" (attempt "+i+")");
            if (System.currentTimeMillis()>maxEndTime)
                throw new IllegalStateException("Unable to ping manager node "+getManagerHostname()+" from "+getMachine()+" for "+entity);
            Time.sleep(1000);
        }
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
      String command = "echo hostname=`hostname`";
      ByteArrayOutputStream stdout = new ByteArrayOutputStream();
      ByteArrayOutputStream stderr = new ByteArrayOutputStream();

      int exitStatus = execute(MutableMap.of("out", stdout, "err", stderr), ImmutableList.of(command), "getHostname");
      String stdouts = new String(stdout.toByteArray());
      String stderrs = new String(stderr.toByteArray());

      Iterable<String> lines = Splitter.on("\n").split(stdouts);
      for (String line : lines) {
         if (line.contains("hostname=") && !line.contains("`hostname`")) {
            return line.substring(line.indexOf("hostname=") + "hostname=".length());
         }
      }

      log.info("No hostname found for {} (got {}; {})", new Object[] { this, stdouts, stderrs });
      return null;
   }

   private InputStream generateAptMirrorFile(String packageManagerMirrorUrl) {
      InputStream mirror = Preconditions.checkNotNull(new ResourceUtils(this).getResourceFromUrl("02proxy"),
            "cannot find 02proxy");
      try {
         String template = CharStreams.toString(new InputStreamReader(mirror));
         String proxy = template.replaceFirst("ip-address", packageManagerMirrorUrl);
         log.debug("PackageManagerProxy set up at: " + proxy);
         return new ByteArrayInputStream(proxy.getBytes("UTF-8"));
      } catch (IOException e) {
         log.error("Cannot generate a mirror file for the package manager in use.", e);
         throw Throwables.propagate(e);
      }
   }
   
   private boolean isPackageManagerAvailable(String packageManagerName) {
      if (log.isTraceEnabled())
         log.trace("Check if {} is available via ssh for {}", packageManagerName, this);
      String command = "which " + packageManagerName;
      ByteArrayOutputStream stdout = new ByteArrayOutputStream();
      ByteArrayOutputStream stderr = new ByteArrayOutputStream();

      int exitStatus = execute(MutableMap.of("out", stdout, "err", stderr), ImmutableList.of(command), "check" + packageManagerName);
      String stdouts = new String(stdout.toByteArray());
      String stderrs = new String(stderr.toByteArray());

      log.info("{} is not available for {} (got {}; {})", new Object[] { packageManagerName, this, stdouts, stderrs });
      return exitStatus == 0 ? true : false;
   }
}
