package io.cloudsoft.cloudera.brooklynnodes;

import java.util.List;
import java.util.Set;

import brooklyn.location.vmware.vcloud.director.client.VCloudDirectorHttpClient;
import brooklyn.location.vmware.vcloud.director.domain.EdgeGateway;
import brooklyn.location.vmware.vcloud.director.domain.EdgeGatewayServiceConfiguration;
import brooklyn.location.vmware.vcloud.director.domain.GatewayInterface;
import brooklyn.location.vmware.vcloud.director.domain.GatewayNatRule;
import brooklyn.location.vmware.vcloud.director.domain.GatewayNatRuleInterface;
import brooklyn.location.vmware.vcloud.director.domain.NatRule;
import brooklyn.location.vmware.vcloud.director.domain.NatService;
import brooklyn.location.vmware.vcloud.director.domain.QueryResultRecords;
import brooklyn.location.vmware.vcloud.director.domain.RuleType;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.ec2.EC2AsyncClient;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.domain.IpProtocol;
import org.jclouds.ec2.domain.Reservation;
import org.jclouds.ec2.domain.RunningInstance;
import org.jclouds.rest.RestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.location.Location;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.net.Cidr;

import com.google.common.collect.Iterables;

import static brooklyn.location.vmware.vcloud.director.client.VCloudDirectorHttpClient.getGatewayInterface;
import static brooklyn.location.vmware.vcloud.director.client.VCloudDirectorHttpClient.tryFindEdgeGatewayId;
import static brooklyn.location.vmware.vcloud.director.client.VCloudDirectorHttpClient.tryFindVdcId;
import static brooklyn.location.vmware.vcloud.director.utils.Networks.getNatRules;

/** Things which are useful, but should be cleaned up, made more protable, and moved to core Brooklyn */ 
public class TempCloudUtils {

    private static final Logger log = LoggerFactory.getLogger(TempCloudUtils.class);
    
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
            }
        }
        log.debug("Skipping ping authorization for "+ssh+"; not required or not supported");
        // TODO for JcloudsSsh and Jclouds -- in AWS
        // see WhirrClusterManager for example
    }

    /** only supports vcloud-director */
    public static void addNatRule(String endpoint, String identity, String credential, String vdcName,
                                  String networkName, String edgeGatewayName, String originalIp,
                                  int originalPort, String translatedIp, int translatedPort) {
        VCloudDirectorHttpClient vCloudDirectorHttpClient =
                new VCloudDirectorHttpClient(endpoint, identity, credential);
        vCloudDirectorHttpClient.login();
        QueryResultRecords queryResultRecords = vCloudDirectorHttpClient.listVDCs();
        String vdcId = tryFindVdcId(queryResultRecords, vdcName);
        queryResultRecords = vCloudDirectorHttpClient.listEdgeGateways(vdcId);
        String edgeGatewayId = tryFindEdgeGatewayId(queryResultRecords, edgeGatewayName);

        EdgeGateway edgeGateway = vCloudDirectorHttpClient.getEdgeGateway(edgeGatewayId);

        EdgeGatewayServiceConfiguration edgeGatewayServiceConfiguration = edgeGateway.getConfiguration()
                .getEdgeGatewayServiceConfiguration();
        NatService natService = edgeGatewayServiceConfiguration.getNatService();
        List<NatRule> natRules = getNatRules(vCloudDirectorHttpClient, edgeGatewayId);

        GatewayInterface gatewayInterface = getGatewayInterface(edgeGateway, networkName);

        GatewayNatRuleInterface gatewayNatRuleInterface =
                GatewayNatRuleInterface.builder()
                        .name(networkName)
                        .href(gatewayInterface.getNetwork().getHref())
                        .build();

        GatewayNatRule gatewayNatRule = GatewayNatRule.builder()
                .gatewayNatRuleInterface(gatewayNatRuleInterface)
                .originalIp(originalIp)
                .originalPort(originalPort)
                .translatedIp(translatedIp)
                .translatedPort(translatedPort)
                .protocol("tcp")
                .build();
        NatRule natRule = NatRule.builder()
                .ruleType(RuleType.DNAT)
                .enabled(true)
                .gatewayNatRule(gatewayNatRule)
                .build();

        natRules.add(natRule);
        natService.setNatRules(natRules);
        edgeGatewayServiceConfiguration.setNatService(natService);

        // add NAT rule
        vCloudDirectorHttpClient.updateNatRules(edgeGatewayId, edgeGatewayServiceConfiguration);
    }

}
