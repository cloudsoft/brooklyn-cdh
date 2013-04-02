package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.GroovyJavaMethods.truth
import brooklyn.entity.Entity;
import brooklyn.entity.proxying.ImplementedBy
import brooklyn.entity.trait.Startable

@ImplementedBy(StartupGroupImpl.class)
public interface StartupGroup extends Entity, Startable {
            
}
