package io.cloudsoft.cloudera.brooklynnodes;

import brooklyn.entity.basic.SoftwareProcessDriver;
import brooklyn.entity.basic.lifecycle.NaiveScriptRunner;
import brooklyn.location.basic.SshMachineLocation;

public interface ClouderaCdhNodeDriver extends SoftwareProcessDriver, NaiveScriptRunner {

    public SshMachineLocation getMachine();
    
}
