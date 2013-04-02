package io.cloudsoft.cloudera.brooklynnodes;

import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.proxying.ImplementedBy;

/** entity representing the Cloudera Manager node which bypasses whirr */ 
@ImplementedBy(DirectClouderaManagerImpl.class)
public interface DirectClouderaManager extends SoftwareProcess, ClouderaManagerNode {

}
