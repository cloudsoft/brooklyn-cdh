package io.cloudsoft.cloudera.brooklynnodes;

import java.io.InputStream;
import java.util.List;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.lifecycle.CommonCommands;
import brooklyn.location.OsDetails;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.ResourceUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
        InputStream rhelRepo = new ResourceUtils(this).getResourceFromUrl("rhel.repo");
        Preconditions.checkNotNull(installCM, "cannot find install_cm.sh script");
        
        List<String> commands = Lists.newArrayList();
        if(getLocation().getOsDetails().getName().equals("RHEL63_ITCS104_V1.1")) {
            getMachine().copyTo(rhelRepo, "/tmp/rhel.repo");
            commands.add(CommonCommands.sudo("sudo /etc/init.d/iptables stop"));
            commands.add(CommonCommands.sudo("cp /tmp/rhel.repo /etc/yum.repos.d/rhel.repo"));
            commands.add(CommonCommands.sudo("yum clean all"));
        }      
        commands.add(CommonCommands.INSTALL_WGET);
        commands.add(CommonCommands.installPackage("expect"));
        getMachine().copyTo(installCM, "/tmp/install_cm.sh");
        commands.add("chmod +x /tmp/install_cm.sh");
        commands.add(CommonCommands.sudo("cp /tmp/install_cm.sh " + getInstallDir()+"/install_cm.sh"));

        newScript(INSTALLING).
//            // ubuntu 12.10 sometimes has "dash" as default shell; it doesn't work!
//            body.append("( if [ -x /bin/bash ] ; then sudo ln -snvf bash /bin/sh ; fi ) || ( echo skipping dash correction )").
            // assumes use of yum to install expect; just install expect ourselves, and wget
            body.append(commands).execute();
    }

    @Override
    public void customize() {
        // no-op
    }

    @Override
    public void launch() {
        newScript(LAUNCHING).failOnNonZeroResultCode().
            body.append(CommonCommands.sudo(
                    "bash -c '( . "+getInstallDir()+"/install_cm.sh && install_cm )'"
//                    ". "+getInstallDir()+"/install_cm.sh && install_cm"
                    )).
            execute();
    }

}
