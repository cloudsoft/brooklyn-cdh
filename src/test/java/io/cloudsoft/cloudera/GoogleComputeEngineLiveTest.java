package io.cloudsoft.cloudera;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import brooklyn.config.BrooklynProperties;
import brooklyn.util.collections.MutableMap;

@Test(groups="Live")
public class GoogleComputeEngineLiveTest extends AbstractCloudLiveTest {

   protected static final Logger log = LoggerFactory.getLogger(GoogleComputeEngineLiveTest.class);


   private static final String NAMED_LOCATION = "gce-us-central1";
   private static final String LOCATION_ID = "us-central1-a";
   private static final String IMAGE_NAME_REGEX = ".*centos-6-v20130325.*";
   private static final String URI = "https://www.googleapis.com/compute/v1beta15/projects/google/global/images/centos-6-v20130325"; 
   private static final String IMAGE_ID = "centos-6-v20130325";    
   
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
            "locationId", LOCATION_ID,
            "imageId", IMAGE_ID, 
            "uri", URI + IMAGE_ID,
            "imageNameRegex", IMAGE_NAME_REGEX
            );
   }

   private String getIdentity() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".identity");
   }
   
   private String getCredential() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".credential");
   }

}
