package io.cloudsoft.cloudera;

import java.util.ArrayList;
import java.util.Map;

import com.google.gson.Gson;

public class Test {
   
   public static void main(String[] args) {
      String jsonServices = "{\"items\":[{\"name\":\"hdfs-CKWiFZjL\",\"type\":\"HDFS\",\"clusterRef\":{\"clusterName\":\"cluster-C1vN5P\"},\"serviceUrl\":\"http://brooklyn-y2cw-andrea-testa-yh67-directclouderam-pegt-1cf.novalocal:7180/cmf/serviceRedirect/hdfs-CKWiFZjL\",\"serviceState\":\"STARTED\",\"healthSummary\":\"CONCERNING\",\"healthChecks\":[],\"configStale\":false,\"maintenanceMode\":false,\"maintenanceOwners\":[],\"displayName\":\"hdfs-CKWiFZjL\"},{\"name\":\"mapreduce-sample\",\"type\":\"MAPREDUCE\",\"clusterRef\":{\"clusterName\":\"cluster-C1vN5P\"},\"serviceUrl\":\"http://brooklyn-y2cw-andrea-testa-yh67-directclouderam-pegt-1cf.novalocal:7180/cmf/serviceRedirect/mapreduce-sample\",\"serviceState\":\"STARTED\",\"healthSummary\":\"GOOD\",\"healthChecks\":[],\"configStale\":false,\"maintenanceMode\":false,\"maintenanceOwners\":[],\"displayName\":\"mapreduce-sample\"},{\"name\":\"zookeeper-IWDCFV3H\",\"type\":\"ZOOKEEPER\",\"clusterRef\":{\"clusterName\":\"cluster-C1vN5P\"},\"serviceUrl\":\"http://brooklyn-y2cw-andrea-testa-yh67-directclouderam-pegt-1cf.novalocal:7180/cmf/serviceRedirect/zookeeper-IWDCFV3H\",\"serviceState\":\"STARTED\",\"healthSummary\":\"GOOD\",\"healthChecks\":[],\"configStale\":false,\"maintenanceMode\":false,\"maintenanceOwners\":[],\"displayName\":\"zookeeper-IWDCFV3H\"}]}";
      Map<String, ArrayList<Map<String, String>>> jsonJavaRootObject = new Gson().fromJson(jsonServices, Map.class);
      ArrayList<Map<String, String>> services = jsonJavaRootObject.get("items");
      System.out.println(services.size());
      for (Map<String, String> service : services) {
         for (String key : service.keySet()) {
            System.out.println("service = " + service);
            if("healthSummary".equals(key)) {
               System.out.println("healthSummary = " + service.get(key));
            }
            if("serviceState".equals(key)) {
               System.out.println("serviceState = " + service.get(key));
            }
         }
      }
   }

}
