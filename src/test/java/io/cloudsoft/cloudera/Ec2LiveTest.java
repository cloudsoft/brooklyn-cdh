package io.cloudsoft.cloudera;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import brooklyn.config.BrooklynProperties;
import brooklyn.util.collections.MutableMap;

@Test(groups="Live")
public class Ec2LiveTest extends AbstractCloudLiveTest {

   protected static final Logger log = LoggerFactory.getLogger(Ec2LiveTest.class);

   private static final String NAMED_LOCATION = "aws-ec2";

   private final BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newDefault();
   
   @Override
   public String getLocation() {
      return NAMED_LOCATION;
   }
   
   @Override
   public Map<String, ?> getFlags() {
      return MutableMap.of(
            "identity", getIdentity(), "credential", getCredential());
   }

   private String getIdentity() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".identity");
   }
   
   private String getCredential() {
      return brooklynProperties.getFirst("brooklyn.location.named." + NAMED_LOCATION + ".credential");
   }

}
