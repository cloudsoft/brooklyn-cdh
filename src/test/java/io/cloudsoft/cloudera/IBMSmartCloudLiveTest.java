package io.cloudsoft.cloudera;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import brooklyn.config.BrooklynProperties;
import brooklyn.util.collections.MutableMap;

@Test(groups="Live")
public class IBMSmartCloudLiveTest extends AbstractCloudLiveTest {

   protected static final Logger log = LoggerFactory.getLogger(IBMSmartCloudLiveTest.class);

   private static final String NAMED_LOCATION = "ibm-smartcloud";
   private static final String REGION_NAME = "RegionOne";
   private static final String LOGIN_USER = "idcuser";
   private static final String HARDWARE_ID = "9";
   private static final String IMAGE_ID = "eeced716-bb37-4f3b-a3d6-977e17f20b21";

   private final BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newDefault();
   
   @Override
   public String getLocation() {
      return NAMED_LOCATION;
   }
   
   @Override
   public Map<String, ?> getFlags() {
      return MutableMap.of(
            "identity", getIdentity(), "credential", getCredential(), 
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
