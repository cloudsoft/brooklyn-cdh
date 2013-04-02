package io.cloudsoft.cloudera.brooklynnodes;

import groovy.transform.InheritConstructors

import brooklyn.entity.Effector
import brooklyn.entity.Entity
import brooklyn.entity.basic.Description
import brooklyn.entity.basic.MethodEffector

@InheritConstructors
public class AllServicesImpl extends StartupGroupImpl implements AllServices {

    /**
     * Start the entity in the given collection of locations.
     */
    @Description("Collect metrics files from all hosts and save to a file on this machine, returning the name of that subdir")
    public String collectMetrics() {
        String name = "cloudera-metrics-"+System.currentTimeMillis();
        String targetBaseDir = "/tmp/cloudera-metrics/";
        new File(targetBaseDir).mkdir();
        String targetDir = targetBaseDir+"/"+name;
        new File(targetDir).mkdir();
        List<ClouderaCdhNode> nodes = [];
        collectNodes(getApplication(), nodes);
        nodes.each { it.collectMetrics(targetDir); }
        return targetDir;
    }
    
    protected void collectNodes(Entity root, List<ClouderaCdhNode> list) {
        if (root in ClouderaCdhNode) list.add(root);
        else for (Entity child: root.ownedChildren) {
            collectNodes(child, list);
        }
    }

}
