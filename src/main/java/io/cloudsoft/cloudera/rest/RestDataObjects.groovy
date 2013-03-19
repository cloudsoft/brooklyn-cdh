package io.cloudsoft.cloudera.rest;

import java.lang.reflect.Field
import java.lang.reflect.Modifier

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.util.flags.FlagUtils
import brooklyn.util.text.Identifiers;
import brooklyn.util.text.Strings;

public class RestDataObjects {

    private static final Logger log = LoggerFactory.getLogger(RestDataObjects.class);
    
    enum ClusterType { CDH3, CDH4 }
    enum ServiceType { HDFS, MAPREDUCE, HBASE, OOZIE, ZOOKEEPER, HUE, YARN }
    
    enum HdfsRoleType { DATANODE, NAMENODE, SECONDARYNAMENODE, BALANCER, GATEWAY, HTTPFS, FAILOVERCONTROLLER }
    enum MapReduceRoleType { JOBTRACKER, TASKTRACKER, GATEWAY }
    enum HBaseRoleType { MASTER, REGIONSERVER, GATEWAY }
    enum ZookeeperRoleType { SERVER }
    
//    YARN    RESROUCEMANAGER, NODEMANAGER, JOBHISTORY, GATEWAY
//    OOZIE   OOZIE_SERVER
//    HUE   HUE_SERVER, BEESWAX_SERVER, KT_RENEWER,   JOBSUBD (CDHv3 only)
    
    public abstract static class Jsonable {
        public abstract Object asJsonType();
    }
    
    public static class Mappable extends Jsonable {
        public Map asJsonType() { return asMap(false); }
        public Map asMap(boolean includeNulls=true) {
            def thiz = this;
            return FlagUtils.getAllFields(thiz.getClass(), { Field f -> 
                        ((f.getModifiers()&(Modifier.PUBLIC|Modifier.STATIC))==Modifier.PUBLIC) && (includeNulls || f.get(thiz)!=null) }).
                    collectEntries { [ it.getName(), RestDataObjects.toJsonType(it.get(thiz)) ] };
        }
    }
    
    public static class HostAddInfo extends Mappable {
        public String hostId, ipAddress, hostname, hostUrl;
    }

    public static class ClusterAddInfo extends Mappable {
        public String name, version;
    }
    
    public static class ConfigInfo extends Mappable {
        public String name, displayName, description, relatedName;
        public boolean required;
        public Object value;
        // TODO need a better strategy, for upstream and downstream...
//        "default" : "...",
//        "validationState" : "OK",
//        "validationMessage" : "..."
    }
    public static class RoleTypeConfigInfo extends Mappable {
        public String roleType;
        public List<ConfigInfo> items;
    }
    public static class ServiceConfigInfo extends Mappable {
        public List<RoleTypeConfigInfo> roleTypeConfigs;
        public List<ConfigInfo> items;
    }
    
    public static class ServiceRoleHostInfo extends Mappable {
        public static ServiceRoleHostInfo newInstance(String clusterName, Object roleType, String hostId) {
            return new ServiceRoleHostInfo(RestDataObjects.tidy(
                Strings.shortener().separator("-").
                    append( "cluster", clusterName).
                    append( "role", (""+roleType).toLowerCase() ).
                    append( "host", hostId ).
                    append( "salt", Identifiers.makeRandomId(4) ).
                    canTruncate( "host", 12 ).
                    canTruncate( "role", 12 ).
                    canTruncate( "cluster", 12 ).
                    canTruncate( "host", 4 ).
                    canTruncate( "cluster", 4 ).
                    getStringOfMaxLength(56))
//                // FIXME use smart shortener (cf jclouds location); cm server has max 63 chars
//                // format is eg. mapreduce_sample_jobtracker_domU_12_31_39_0E_C6_A3_compute_1_internal
//                (clusterName?clusterName+"-":"")+""+(""+roleType).toLowerCase()+"-"+
//                    hostId.substring(0, hostId.length()>16 ? 16 : hostId.length())+
//                (clusterName?"":"-"+Strings.makeRandomId(4))), 
                ,
                ""+roleType, hostId);
        }
        public ServiceRoleHostInfo(Object roleType, String hostId) {
            this(RestDataObjects.tidy((""+roleType).toLowerCase()+"_"+hostId), ""+roleType, hostId);
        }
        public ServiceRoleHostInfo(String roleName, Object roleType, String hostId) {
            name = roleName;
            type = ""+roleType;
            hostRef = [hostId: hostId];
        }
        
        public String name, type;
        public Map hostRef;
    }

    public static class RemoteCommand {
        // TODO should extend Future, maybe Task ?
        ClouderaRestCaller caller; 
        int id; 
        String name;
        
        public RemoteCommand(ClouderaRestCaller caller, int id, String name) {
            this.caller = caller;
            this.id = id;
            this.name = name;
        }
        
        public String toString() {
            return "Command "+id+": "+name;
        }
        
        public boolean isActive() {
            caller.getCommandJson(id).active;
        }

        public boolean block(long millis) {
            // TODO this is a poor man's block...
            long startTime = System.currentTimeMillis();        
            while (isActive()) {
                if (millis<0 || System.currentTimeMillis() > startTime + millis)
                    return false;
                log.debug("waiting for "+this+" ("+(System.currentTimeMillis()-startTime)+"ms elapsed)");
                Thread.sleep(1000);
            }
            return true;
        }
        
        public static RemoteCommand fromJson(Object x, ClouderaRestCaller caller) {
            return new RemoteCommand(caller, x.id, x.name);
        }
    }
    
    public static class RemoteCommandSet {
        List<RemoteCommand> commands;
        
        public boolean block(long millis) {
            // TODO very poor man's block
            long endTime = System.currentTimeMillis() + millis;
            for (RemoteCommand c : commands) {
                if (!c.block(millis <=0 ? millis : Math.max(endTime - System.currentTimeMillis(), 0)))
                    return false;
            }
            return true;
        }
        
        public static RemoteCommandSet fromJson(Object x, ClouderaRestCaller caller) {
            return new RemoteCommandSet(commands: x.items.collect { RemoteCommand.fromJson(it, caller) });
        }
    }
 
    /** converts a map of ServiceConfigInfo to the minimum required for _setting_ */
    public static Map convertConfigForSetting(Object config, String context) {
        Map newConfig = [:];
        
        newConfig.roleTypeConfigs = 
            config.roleTypeConfigs.collect({
                Map newRtConfig = [:];
                newRtConfig.roleType = it.roleType;
                newRtConfig.items = []
                newRtConfig.items.addAll it.items.findAll({ it.containsKey("value") && it.value!=null }).
                    collect { [name:it.name, value:it.value] };
                newRtConfig.items.addAll it.items.findAll({ it.required && (!it.containsKey("value") || it.value==null) }).
                    collect { ci -> convertConfigItemForSetting(ci, context+"-"+it.roleType); }
                return newRtConfig;
            }).findAll({it.items})
        newConfig.items = []
        newConfig.items.addAll config.items.findAll({ it.containsKey("value") && it.value!=null }).
            collect { [name:it.name, value:it.value] };
        newConfig.items.addAll config.items.findAll({ it.required && (!it.containsKey("value") || it.value==null) })
            .collect { convertConfigItemForSetting(it, context); }
        return newConfig;
    }
    public static Map convertConfigItemForSetting(Object config, String context) {
        return [name: config.name, value: getDefaultConfigValue(config.name, context, config)]
    }
    public static Object getDefaultConfigValue(String keyName, String context, Object config) {
        if (keyName.endsWith(".dir") || keyName.endsWith("_dir") || keyName.endsWith("_dir_list")) {
            return "/mnt/"+tidy(context)+"/"+tidy(keyName);
        }
        log.warn("Unknown configuration key "+keyName+"  ("+context+" with "+config);
        return "UNKNOWN-"+tidy(context)+"-"+tidy(keyName);
    }
    public static List setConfig(Object config, String key, Object value) {
        List occurrences = new ArrayList(config.items.findAll({it.name==key}));
        if (config.roleTypeConfigs) occurrences.addAll(config.roleTypeConfigs.items.findAll({it.name==key}));
        occurrences.each { it.value = value };
        return occurrences;
    }
    public static List setRoleConfig(Object config, String role, String key, Object value) {
        List occurrences = []
        if (config.roleTypeConfigs) {
            config.roleTypeConfigs.findAll({role.equalsIgnoreCase(it.roleType)}).
                each({it.items.findAll({it.name==key}).each{ occurrences.add(it) } });
        }
        occurrences.each { it.value = value };
        return occurrences;
    }
    public static List setMetricsRoleConfig(Object config, String service, String role) {
        def configSet1 = setRoleConfig(config, role, "hadoop_metrics2_safety_valve",
            "*.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink\n"+"\n"+
            role.toLowerCase()+".sink.file.filename=/tmp/"+role.toLowerCase()+"-metrics.out\n"
        );
        def configSet2 = setRoleConfig(config, role, "hadoop_metrics_safety_valve",
            """mapred.class=org.apache.hadoop.metrics.file.FileContext
mapred.period=10
mapred.fileName=/tmp/mrmetrics.log
jvm.class=org.apache.hadoop.metrics.file.FileContext
jvm.period=10
jvm.fileName=/tmp/jvmmetrics.log
""");
    
        if (configSet1) 
            log.info("Enabled metrics2 config for "+role+" nodes in "+service);
        else if (configSet2) 
            log.info("Enabled metrics config for "+role+" nodes in "+service);
        else
            log.warn("No configuration found to enable metrics for "+role+" nodes in "+service);
        return configSet1+configSet2;
    }
       
    /** converts strings of anything but alphanums and _ to a single - (and collapsing multiple such);
     * NB '-' (hyphen) removed as valid char for v2 API because it implies a very specific format */
    public static String tidy(String s) {
        return s.replaceAll("[^_A-Za-z0-9]+","_");
    }
    public static Object toJsonType(Object x) {
        if (x in String) return x;
        if (x in Jsonable) return toJsonType(x.asJsonType());
        if (x in Map) {
            Map x2 = [:];
            x.each { k,v -> x2.put(k, toJsonType(v)) }
            return x2;
        }
        if (x in Collection) {
            return x.collect { toJsonType(it) }
        }
        // assume other types are simple
        return x;
    }
        
}