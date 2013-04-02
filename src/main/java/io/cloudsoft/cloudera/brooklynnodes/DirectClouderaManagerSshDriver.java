package io.cloudsoft.cloudera.brooklynnodes;

import java.io.InputStream;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.lifecycle.CommonCommands;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.ResourceUtils;

import com.google.common.base.Preconditions;

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
        InputStream installCM = new ResourceUtils(this).getResourceFromUrl("functions/install_cm.sh");
        Preconditions.checkNotNull(installCM, "cannot find install_cm.sh script");
        newScript(INSTALLING).
//            // ubuntu 12.10 sometimes has "dash" as default shell; it doesn't work!
//            body.append("( if [ -x /bin/bash ] ; then sudo ln -snvf bash /bin/sh ; fi ) || ( echo skipping dash correction )").
            // assumes use of yum to install expect; just install expect ourselves, and wget
            body.append(
                    CommonCommands.INSTALL_WGET,
                    CommonCommands.installPackage("expect")).
            execute();
        getMachine().copyTo(installCM, getInstallDir()+"/install_cm.sh");
        newScript(INSTALLING+":setExecutable").
            body.append("chmod +x "+getInstallDir()+"/install_cm.sh").
            execute();
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
