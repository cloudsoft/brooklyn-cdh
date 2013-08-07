package io.cloudsoft.cloudera;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import brooklyn.config.BrooklynProperties;
import brooklyn.util.collections.MutableMap;

@Test(groups="Live")
public class SoftlayerLiveTest extends AbstractCloudLiveTest {

   protected static final Logger log = LoggerFactory.getLogger(SoftlayerLiveTest.class);

   private static final String NAMED_LOCATION = "softlayer";
   
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
            "minRam", 3,
            "imageId", 13945,
            "locationId", 265592,
            "vmNameMaxLength", 30
            );
   }

   private String getIdentity() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".identity");
   }
   
   private String getCredential() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".credential");
   }

}
