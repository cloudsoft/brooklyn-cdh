package io.cloudsoft.cloudera.brooklynnodes;

import brooklyn.entity.Effector
import brooklyn.entity.basic.MethodEffector
import brooklyn.entity.proxying.ImplementedBy

@ImplementedBy(AllServicesImpl.class)
public interface AllServices extends StartupGroup {

    public static final Effector<String> COLLECT_METRICS = new MethodEffector<String>(this.&collectMetrics);

    public String collectMetrics();
    
}
