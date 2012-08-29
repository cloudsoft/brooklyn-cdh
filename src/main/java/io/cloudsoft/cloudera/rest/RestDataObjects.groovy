package io.cloudsoft.cloudera.rest;

import java.lang.reflect.Field
import java.lang.reflect.Modifier

import brooklyn.util.flags.FlagUtils

public class RestDataObjects {

    enum ClusterType { CDH3, CDH4 }
    enum ServiceType { HDFS, MAPREDUCE, HBASE, OOZIE, ZOOKEEPER, HUE, YARN }
    
    enum HdfsRoleType { DATANODE, NAMENODE, SECONDARYNAMENODE, BALANCER, GATEWAY, HTTPFS, FAILOVERCONTROLLER }
    
//    MAPREDUCE   JOBTRACKER, TASKTRACKER, GATEWAY
//    HBASE   MASTER, REGIONSERVER, GATEWAY
//    YARN    RESROUCEMANAGER, NODEMANAGER, JOBHISTORY, GATEWAY
//    OOZIE   OOZIE_SERVER
//    ZOOKEEPER   SERVER
//    HUE   HUE_SERVER, BEESWAX_SERVER, KT_RENEWER,   JOBSUBD (v3 only)
    
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
        public ServiceRoleHostInfo(Object roleType, String hostId) {
            this(RestDataObjects.tidy((""+roleType).toLowerCase()+"-"+hostId), ""+roleType, hostId);
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
            // TODO very poor man's block
            long startTime = System.currentTimeMillis();        
            while (isActive()) {
                if (millis<0 || System.currentTimeMillis() > startTime + millis)
                    return false;
                println "waiting for "+this;
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
                newRtConfig.items = it.items.findAll({ it.required && (!it.containsKey("value") || it.value==null) }).collect { 
                    ci -> convertConfigItemForSetting(ci, context+"-"+it.roleType);
                }
                return newRtConfig;
            }).findAll({it.items})
        newConfig.items =
            config.items.findAll({ it.required && (!it.containsKey("value") || it.value==null) }).collect {
                convertConfigItemForSetting(it, context);
            }
        return newConfig;
    }
    public static Map convertConfigItemForSetting(Object config, String context) {
        return [name: config.name, value: getDefaultConfigValue(config.name, context, config)]
    }
    public static Object getDefaultConfigValue(String keyName, String context, Object config) {
        if (keyName.endsWith(".dir") || keyName.endsWith("_dir") || keyName.endsWith("_dir_list")) {
            return "/mnt/"+tidy(context)+"/"+tidy(keyName);
        }
        return "UNKNOWN-"+tidy(context)+"-"+tidy(keyName);
    }
       
    /** converts strings of anything but alphanums and - and _ to a single - (and collapsing multiple -'s) */
    public static String tidy(String s) {
        return s.replaceAll("-*[^-_A-Za-z0-9]-*","-");
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