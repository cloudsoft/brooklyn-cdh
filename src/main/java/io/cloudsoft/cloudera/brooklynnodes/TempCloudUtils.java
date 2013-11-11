package io.cloudsoft.cloudera.brooklynnodes;

import java.net.URI;
import java.util.Set;

import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.ec2.EC2AsyncClient;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.domain.IpProtocol;
import org.jclouds.ec2.domain.Reservation;
import org.jclouds.ec2.domain.RunningInstance;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApi;
import org.jclouds.googlecomputeengine.domain.Firewall;
import org.jclouds.googlecomputeengine.domain.Firewall.Rule;
import org.jclouds.googlecomputeengine.options.FirewallOptions;
import org.jclouds.rest.RestContext;
import org.jclouds.rest.internal.ApiContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.location.Location;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.net.Cidr;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/** Things which are useful, but should be cleaned up, made more protable, and moved to core Brooklyn */ 
public class TempCloudUtils {

    private static final Logger log = LoggerFactory.getLogger(TempCloudUtils.class);
    private static final String NETWORK_API_URL_SUFFIX = "/global/networks/";
    
    /** only supports EC2 */
    public static void authorizePing(Cidr cidr, Location ssh) {
        JcloudsLocation jcl = null;
        JcloudsSshMachineLocation jclssh = null;
        if (ssh instanceof JcloudsSshMachineLocation) {
            jclssh = ((JcloudsSshMachineLocation)ssh);
            jcl = jclssh.getParent();
        }
        if (jcl!=null) {
            ComputeServiceContext ctx = jcl.getComputeService().getContext();
            if (ctx.unwrap().getProviderMetadata().getId().equals("aws-ec2")) {
               String region = jcl.getRegion();
                try {
                    RestContext<EC2Client, EC2AsyncClient> ec2Client = ctx.unwrap();
                    String id = jclssh.getNode().getId();
                    // string region prefix from id
                    if (id.indexOf('/')>=0) id = id.substring(id.indexOf('/')+1);
                    Set<? extends Reservation<? extends RunningInstance>> instances = ec2Client.getApi().getInstanceServices().describeInstancesInRegion(region, id);
                    Set<String> groupNames = Iterables.getOnlyElement(instances).getGroupNames(); 
                    String groupName = Iterables.getFirst(groupNames, null);
                    // "jclouds#" + clusterSpec.getClusterName(); // + "#" + region;
                    log.info("Authorizing ping for "+groupName+": "+cidr);
                    ec2Client.getApi().getSecurityGroupServices().authorizeSecurityGroupIngressInRegion(region, 
                          groupName, IpProtocol.ICMP, -1, -1, cidr.toString());
                    return;
                } catch(Exception e) {
                    log.warn("Problem authorizing ping (possibly already authorized) for "+ssh+": "+e.getMessage());
                    log.debug("Details for problem authorizing ping (possibly already authorized) for "+ssh+": "+e.getMessage(), e);
                    /* ignore, usually means that this permission was already granted */
                    Exceptions.propagateIfFatal(e);
                }
            } else if (ctx.unwrap().getProviderMetadata().getId().equals("google-compute-engine")) {
                String group = jclssh.getNode().getGroup();
                String network = String.format("jclouds-%s", group);
                String firewallName = network;
                String projectName = Iterables.get(Splitter.on("/").split(jclssh.getNode().getUri().toASCIIString()), 6);
                String apiUrlPrefix = Iterables.get(Splitter.on(projectName).split(jclssh.getNode().getUri().toASCIIString()), 0);
                try {
                    ApiContextImpl<GoogleComputeEngineApi> gce = ctx.unwrap();
                    Set<Rule> alreadyAllowedRules = gce.getApi().getFirewallApiForProject(projectName).get(firewallName).getAllowed();
                    Set<Rule> allowedRules = Sets.newHashSet();
                    allowedRules .add(Firewall.Rule.builder()
                            .IPProtocol(Firewall.Rule.IPProtocol.ICMP)
                            .build());
                    allowedRules.addAll(alreadyAllowedRules);
                    FirewallOptions firewallOptions = new FirewallOptions()
                        .name(firewallName)
                        .network(URI.create(apiUrlPrefix + projectName + NETWORK_API_URL_SUFFIX + network))
                        .sourceRanges(ImmutableSet.of("0.0.0.0/0","10.0.0.0/8"))
                        .allowedRules(allowedRules);
                    gce.getApi().getFirewallApiForProject(projectName).update(firewallName, firewallOptions);
                    return;
                } catch(Exception e) {
                    log.warn("Problem authorizing ping (possibly already authorized) for "+ssh+": "+e.getMessage());
                    log.debug("Details for problem authorizing ping (possibly already authorized) for "+ssh+": "+e.getMessage(), e);
                    /* ignore, usually means that this permission was already granted */
                    Exceptions.propagateIfFatal(e);
                }
            }
        }
        log.debug("Skipping ping authorization for "+ssh+"; not required or not supported");
        // TODO for JcloudsSsh and Jclouds -- in AWS
        // see WhirrClusterManager for example
    }

}
