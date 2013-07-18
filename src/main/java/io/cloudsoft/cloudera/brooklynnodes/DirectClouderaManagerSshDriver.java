package io.cloudsoft.cloudera.brooklynnodes;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;

import joptsimple.internal.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.ibm.smartcloud.IbmSmartLocationConfig;
import brooklyn.util.ResourceUtils;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.internal.Repeater;
import brooklyn.util.internal.ssh.SshTool;
import brooklyn.util.ssh.CommonCommands;
import brooklyn.util.time.Time;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;

public class DirectClouderaManagerSshDriver extends AbstractSoftwareProcessSshDriver implements DirectClouderaManagerDriver {

    private static final Logger LOG = LoggerFactory.getLogger(DirectClouderaManagerSshDriver.class);
   
    private static final String YUM = "yum";
    private static final String APT_GET = "apt-get";

    public DirectClouderaManagerSshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public boolean isRunning() {
        int result = newScript(CHECK_RUNNING).
            body.append("ps aux | grep -i java").
            execute();
        return (result == 0);
    }

    @Override
    public void stop() {
        newScript(KILLING).
                body.append("killall java").
                execute();
    }

    @Override
    public void install() {
        entity.setAttribute(DirectClouderaManager.LOCAL_HOSTNAME, execHostname());
        
        InputStream installCM = new ResourceUtils(this).getResourceFromUrl("install_cm.sh");
        Preconditions.checkNotNull(installCM, "cannot find install_cm.sh script");
        String aptProxyUrl = getLocation().getConfig(ClouderaManagerNode.APT_PROXY);
        String yumMirrorUrl = getLocation().getConfig(ClouderaManagerNode.YUM_MIRROR);
        if(!Strings.isNullOrEmpty(aptProxyUrl)) {
            InputStream proxy = generatePackageManagerProxyFile(aptProxyUrl, APT_GET);
            getMachine().copyTo(proxy, "/tmp/02proxy");
            newScript(INSTALLING+":setAptProxy").setFlag(SshTool.PROP_ALLOCATE_PTY.getName(), true).
            body.append(CommonCommands.sudo("mv /tmp/02proxy /etc/apt/apt.conf.d/02proxy")).execute();
        }
        if(!Strings.isNullOrEmpty(yumMirrorUrl)) {
            InputStream proxy = generatePackageManagerProxyFile(yumMirrorUrl, YUM);
            getMachine().copyTo(proxy, "/tmp/cloudera-manager.repo");
            newScript(INSTALLING+":setYumProxy").setFlag(SshTool.PROP_ALLOCATE_PTY.getName(), true).
            body.append(
                    CommonCommands.sudo("mv /tmp/cloudera-manager.repo /etc/yum.repos.d/cloudera-manager.repo"),
                    CommonCommands.sudo("yum update"))
                    .execute();
        }
        
        newScript(INSTALLING).
//            // ubuntu 12.10 sometimes has "dash" as default shell; it doesn't work!
//            body.append("( if [ -x /bin/bash ] ; then sudo ln -snvf bash /bin/sh ; fi ) || ( echo skipping dash correction )").
            // assumes use of yum to install expect; just install expect ourselves, and wget
            body.append(
                    CommonCommands.INSTALL_WGET,
                    CommonCommands.installPackage("expect")).
            execute();
        getMachine().copyTo(installCM, getInstallDir()+"/install_cm.sh");

        
        newScript(INSTALLING+":setExecutable").setFlag(SshTool.PROP_ALLOCATE_PTY.getName(), true).
        body.append("chmod +x "+getInstallDir()+"/install_cm.sh").execute();
    }

    private InputStream generatePackageManagerProxyFile(String url, String packageManagerName) {
        if (packageManagerName.equals(APT_GET)) {
            try {
                InputStream proxy = Preconditions.checkNotNull(new ResourceUtils(this).getResourceFromUrl("02proxy"), "cannot find 02proxy");
                String template = CharStreams.toString(new InputStreamReader(proxy));
                String aptProxy = template.replaceFirst("ip-address", url);
                log.debug("PackageManagerProxy set up at: " + aptProxy);
                return new ByteArrayInputStream(aptProxy.getBytes("UTF-8"));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else if (packageManagerName.equals(YUM)) {
            try {
                InputStream proxy = Preconditions.checkNotNull(new ResourceUtils(this).getResourceFromUrl("cloudera-manager.repo"), "cannot find cloudera-manager.repo");
                String template = CharStreams.toString(new InputStreamReader(proxy));
                String yumMirror = template.replaceFirst("ip-address", url);
                log.debug("PackageManagerProxy set up at: " + yumMirror);
                return new ByteArrayInputStream(yumMirror.getBytes("UTF-8"));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void customize() {
        // no-op
    }

    @Override
    public void launch() {
        // particularly useful for IBM SCE
        if (getLocation().getConfig(IbmSmartLocationConfig.SELINUX_DISABLED)) {
            log.debug("Disable SELINUX");
            newScript(LAUNCHING+":disableSELINUX").setFlag(SshTool.PROP_ALLOCATE_PTY.getName(), true).
            body.append(
                    CommonCommands.sudo("sed -i \"s/SELINUX=/SELINUX=disabled # it was /\" /etc/selinux/config"),
                    CommonCommands.sudo("reboot"))
                    .execute();
            Time.sleep(10*1000L);
            waitForSshable(getLocation(), getLocation().getConfig(IbmSmartLocationConfig.SSH_REACHABLE_TIMEOUT_MILLIS));
        }
        
        newScript(LAUNCHING).
            body.append(
                    CommonCommands.sudo(
                    "bash -c '( . "+getInstallDir()+"/install_cm.sh && install_cm )'"
                    )).
            execute();
    }
    
    private void waitForSshable(final SshMachineLocation machine, long delayMs) {
        boolean reachable = new Repeater()
            .repeat()
            .every(1,SECONDS)
            .until(new Callable<Boolean>() {
                public Boolean call() {
                    return machine.isSshable();
                }})
            .limitTimeTo(delayMs, MILLISECONDS)
            .run();

        if (!reachable) {
            throw new IllegalStateException("SSH failed for "+
                    machine.getUser()+"@"+machine.getAddress()+" (" + machine.getDisplayName() +") after waiting "+
                    Time.makeTimeStringRounded(delayMs));
        }
    }

    // TODO Move up to a super-type
    private String execHostname() {
        if (LOG.isTraceEnabled()) LOG.trace("Retrieve `hostname` via ssh for {}", this);
        String command = "echo hostname=`hostname`";
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    
        int exitStatus = execute(MutableMap.of("out", stdout, "err", stderr), ImmutableList.of(command), "getHostname");
    String stdouts = new String(stdout.toByteArray());
    String stderrs = new String(stderr.toByteArray());
    
    Iterable<String> lines = Splitter.on("\n").split(stdouts);
    for (String line : lines) {
      if (line.contains("hostname=") && !line.contains("`hostname`")) {
        return line.substring(line.indexOf("hostname=")+"hostname=".length());
      }
    }
    
    LOG.info("No hostname found for {} (got {}; {})", new Object[] {this, stdouts, stderrs});
    return null;
    }
}
