package io.cloudsoft.cloudera;

import static brooklyn.entity.proxying.EntitySpecs.spec;
import io.cloudsoft.cloudera.brooklynnodes.ClouderaManagerNode;
import io.cloudsoft.cloudera.brooklynnodes.DirectClouderaManager;
import io.cloudsoft.cloudera.rest.ClouderaRestCaller;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import brooklyn.config.BrooklynProperties;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.ApplicationBuilder;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.trait.Startable;
import brooklyn.location.Location;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.management.ManagementContext;
import brooklyn.management.internal.LocalManagementContext;
import brooklyn.test.EntityTestUtils;
import brooklyn.test.entity.TestApplication;
import brooklyn.util.collections.MutableMap;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;

public abstract class AbstractCloudLiveTest {
   
   
   private static final String STARTED = "STARTED";
   private static final String GOOD = "GOOD";
   
   private static final String CDH_USER = "admin";
   private static final String CDH_PASSWORD = "admin";
   private ManagementContext ctx;
   private Location location;
   private TestApplication app;
   
   protected static List<SshMachineLocation> machines;
   private static SampleClouderaManagedClusterInterface cdhApp;
   
   private boolean isCertificationCluster = true;
   private boolean includeHbase = false;
   
   private ClouderaRestCaller caller;
   
   protected static final Logger log = LoggerFactory.getLogger(AbstractCloudLiveTest.class);

   @BeforeClass
   public void beforeClass() throws Exception {
      machines = Lists.newArrayList();
       try {
           ctx = new LocalManagementContext();
           app = ApplicationBuilder.newManagedApp(TestApplication.class, ctx);
           cdhApp = app.createAndManageChild(spec(SampleClouderaManagedClusterInterface.class));
           Map<String, ?> flags = getFlags();
           location = ctx.getLocationRegistry().resolve(getLocation(), flags);
           log.info("Started CDH deployment on '" + location +"'");
           app.start(Arrays.asList(location));
           EntityTestUtils.assertAttributeEqualsEventually(app, Startable.SERVICE_UP, true);
       } catch (Exception e) {
           log.error("Failed to deploy CDH", e);
           throw e;
       }
   }

   @AfterClass
   public void afterClass() throws Exception {
      //app.stop();
      Entities.destroyAll(ctx);
   }
   
   public abstract String getLocation();
   public abstract Map<String, ?> getFlags();

   @Test(groups="Live")
   public void deployServices() throws MalformedURLException {
      cdhApp.startServices(isCertificationCluster, includeHbase);
      String server = "";
      if(getClouderaManagerUrl().isPresent())
         server = new URL(getClouderaManagerUrl().get()).getHost();
      else {
         Assert.fail();
      }
      caller = ClouderaRestCaller.newInstance(server, CDH_USER, CDH_PASSWORD);
      String clusterName = Iterables.getFirst(caller.getClusters(), "");
      String jsonServices = caller.getServicesJson(clusterName).toString();
      validateServices(jsonServices);
   }
   
   private void validateServices(String jsonServices) {
      Map<String, ArrayList<Map<String, String>>> jsonJavaRootObject = new Gson().fromJson(jsonServices, Map.class);
      ArrayList<Map<String, String>> services = jsonJavaRootObject.get("items");
      Assert.assertEquals(services.size(), 3);
      for (Map<String, String> service : services) {
         for (String key : service.keySet()) {
            log.debug("service {}", service);
            if("healthSummary".equals(key)) {
               String healthSummary = service.get(key);
               log.debug("\thealthSummary {}", healthSummary);
               Assert.assertEquals(healthSummary, GOOD);
            }
            if("serviceState".equals(key)) {
               String serviceState = service.get(key);
               log.debug("\tserviceState {}", serviceState);
               Assert.assertEquals(serviceState, STARTED);
            }
         }
      }
   }

   private static Optional<String> getClouderaManagerUrl() {
      for (Entity child : cdhApp.getChildren()) {
         if (child.getEntityType().getName().contains("StartupGroup")) {
            for (Entity node : child.getChildren()) {
               if (node.getEntityType().getName().contains("DirectClouderaManager")) {
                  return Optional.of(((DirectClouderaManager) node)
                        .getAttribute(ClouderaManagerNode.CLOUDERA_MANAGER_URL));
               }
            }
         }
      }
      return Optional.absent();
   }
}
