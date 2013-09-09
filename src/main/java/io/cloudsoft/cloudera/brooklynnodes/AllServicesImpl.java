package io.cloudsoft.cloudera.brooklynnodes;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;

public class AllServicesImpl extends StartupGroupImpl implements AllServices {

    public AllServicesImpl() {
    }

    /**
     * Start the entity in the given collection of locations.
     */
    @Effector(description="Collect metrics files from all hosts and save to a file on this machine, returning the name of that subdir")
    public String collectMetrics() {
        String name = "cloudera-metrics-"+System.currentTimeMillis();
        String targetBaseDir = "/tmp/cloudera-metrics/";
        new File(targetBaseDir).mkdir();
        String targetDir = targetBaseDir+"/"+name;
        new File(targetDir).mkdir();
        List<ClouderaCdhNode> nodes = new ArrayList<ClouderaCdhNode>();
        collectNodes(getApplication(), nodes);
        for (ClouderaCdhNode it: nodes) { it.collectMetrics(targetDir); }
        return targetDir;
    }
    
    protected void collectNodes(Entity root, List<ClouderaCdhNode> list) {
        if (root instanceof ClouderaCdhNode) list.add((ClouderaCdhNode) root);
        else for (Entity child: root.getChildren()) {
            collectNodes(child, list);
        }
    }

}
