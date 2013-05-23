package io.cloudsoft.cloudera.brooklynnodes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import joptsimple.internal.Strings;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.ResourceUtils;
import brooklyn.util.internal.ssh.SshTool;
import brooklyn.util.ssh.CommonCommands;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;

public class DirectClouderaManagerSshDriver extends AbstractSoftwareProcessSshDriver implements DirectClouderaManagerDriver {

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
        InputStream installCM = new ResourceUtils(this).getResourceFromUrl("install_cm.sh");
        Preconditions.checkNotNull(installCM, "cannot find install_cm.sh script");
        String aptProxyUrl = getLocation().getConfig(ClouderaManagerNode.APT_PROXY);
        if(!Strings.isNullOrEmpty(aptProxyUrl)) {
            InputStream proxy = generatePackageManagerProxyFile(aptProxyUrl);
            getMachine().copyTo(proxy, "/tmp/02proxy");
            newScript(INSTALLING+":setAptProxy").setFlag(SshTool.PROP_ALLOCATE_PTY.getName(), true).
            body.append(CommonCommands.sudo("mv /tmp/02proxy /etc/apt/apt.conf.d/02proxy")).execute();
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

    @Override
    public void customize() {
        // no-op
    }

    @Override
    public void launch() {
        newScript(LAUNCHING).
            body.append(CommonCommands.sudo(
                    "bash -c '( . "+getInstallDir()+"/install_cm.sh && install_cm )'"
//                    ". "+getInstallDir()+"/install_cm.sh && install_cm"
                    )).
            execute();
    }

}
