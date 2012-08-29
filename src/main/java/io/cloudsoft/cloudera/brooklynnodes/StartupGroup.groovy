package io.cloudsoft.cloudera.brooklynnodes;

import groovy.transform.InheritConstructors;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.entity.basic.AbstractEntity
import brooklyn.entity.trait.Startable
import brooklyn.entity.trait.StartableMethods
import brooklyn.location.Location

@InheritConstructors
public class StartupGroup extends AbstractEntity implements Startable {

    public static final Logger log = LoggerFactory.getLogger(StartupGroup.class);

    @Override public void start(Collection<? extends Location> locations) { StartableMethods.start(this, locations); }
    @Override public void stop() { StartableMethods.stop(this); }
    @Override public void restart() { StartableMethods.restart(this); }
    
}
