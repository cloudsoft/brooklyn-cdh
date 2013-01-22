package io.cloudsoft.cloudera.rest;

import io.cloudsoft.test.rest.SimpleWebForTesting
import io.cloudsoft.test.rest.SimpleWebRequestProcessor
import io.cloudsoft.test.rest.SimpleWebResponder

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testng.Assert;
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test;

public class ClouderaRestCallerTest {

    private static final Logger log = LoggerFactory.getLogger(ClouderaRestCallerTest.class);
            
    SimpleWebForTesting server;
    ClouderaRestCaller caller;
    
    private class MockClouderaWebRequestProcessor implements SimpleWebRequestProcessor {
        @Override
        public Object handle(String path, Map query, SimpleWebResponder responder) {
            if (path.startsWith("api/v2/")) {
                path = path.substring(7);
                if ("clusters".equals(path)) return responder.json([items:[[name:"Cluster 1", version:"CDH4"]]]);
            }
            log.info("unhandled request: "+path);
            return responder.code(404);
        }
    }

    
    @BeforeClass
    public void startServer() {
        server = SimpleWebForTesting.launch(new MockClouderaWebRequestProcessor());
        log.info("started "+server);
        caller = new ClouderaRestCaller(server:"localhost", port:server.getPort(), authName:"admin", authPass:"admin");
    }
    
    @AfterClass
    public void stopServer() {
        log.info("stopping "+server);
        server.close();
    }

    @Test
    public void testGetClusters() {
        Assert.assertEquals(caller.getClusters(), ["Cluster 1"]);
    }

}
