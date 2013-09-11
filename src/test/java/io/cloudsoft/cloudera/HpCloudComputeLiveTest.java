package io.cloudsoft.cloudera;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import brooklyn.config.BrooklynProperties;
import brooklyn.util.collections.MutableMap;

@Test(groups="Live")
public class HpCloudComputeLiveTest extends AbstractCloudLiveTest {

   protected static final Logger log = LoggerFactory.getLogger(HpCloudComputeLiveTest.class);


   private static final String NAMED_LOCATION = "hpcloud-compute";
   private static final String REGION_NAME = "az-1.region-a.geo-1";
   private static final String LOGIN_USER = "ubuntu";
   private static final String HARDWARE_ID = "103";   // standard.large – 4 vCPU / 8 GB RAM / 240 GB HD
   private static final String IMAGE_ID = "81078";    // 81078 – Ubuntu Server 12.04.2 LTS (amd64 20130318) - Partner Image
   
   private final BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newDefault();
   
   @Override
   public String getLocation() {
      return NAMED_LOCATION;
   }
   
   @Override
   public Map<String, ?> getFlags() {
      return MutableMap.of(
            "identity", getIdentity(), 
            "credential", getCredential(), 
            "imageId", REGION_NAME + "/" + IMAGE_ID, 
            "loginUser", LOGIN_USER, "hardwareId", 
            REGION_NAME + "/" + HARDWARE_ID);
   }

   private String getIdentity() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".identity");
   }
   
   private String getCredential() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".credential");
   }

}
