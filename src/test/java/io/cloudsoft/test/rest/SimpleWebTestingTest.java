package io.cloudsoft.test.rest;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import brooklyn.util.collections.MutableMap;
import brooklyn.util.ResourceUtils;

@Test
public class SimpleWebTestingTest {
    
    private static final Logger log = LoggerFactory.getLogger(SimpleWebTestingTest.class);
    
    private class MySimpleWebRequestProcessor implements SimpleWebRequestProcessor {
        @Override
        public Object handle(String path, Map<String,String> query, SimpleWebResponder responder) {
            log.info("my handler, handling "+path);
            if ("json".equals(path)) return responder.json(MutableMap.of("x", 1, "y", "yy"));
            if ("text".equals(path)) return responder.text("foo");
            if ("html".equals(path)) return responder.html("<b>Foo</b>");
            return responder.code(404);
        }
    }

    private SimpleWebForTesting server;

    @BeforeClass
    public void startServer() {
        server = SimpleWebForTesting.launch(new MySimpleWebRequestProcessor());
        log.info("started "+server);
    }
    
    @AfterClass
    public void stopServer() {
        log.info("stopping "+server);
        server.close();
    }

    // long style of test
    public void testHtmlLong() throws Exception {
        HttpURLConnection c = ((HttpURLConnection) new URL(server.getUrl()+"html/").openConnection());
        Assert.assertEquals(c.getResponseCode(), 200);
        String content = ResourceUtils.readFullyString( (InputStream)c.getContent() );
        Assert.assertEquals(content, "<b>Foo</b>");
        Assert.assertEquals(c.getContentType(), "text/html");
    }

    // shorthand, using SimpleWebConnectionAsserter
    public void testHtmlSimple() throws Exception {
        server.get("html/").
            assertResponseCode(200).
            assertContentType("text/html").
            assertContentContains("Foo");
    }

    public void testTextSimple() throws Exception {
        server.get("text/").
            assertResponseCode(200).
            assertContentType("text/plain").
            assertContentEquals("foo");
    }

    @SuppressWarnings("rawtypes")
    public void testAppJson() throws Exception {
        SimpleWebConnectionAsserter asserter = server.get("json/");
        asserter.assertResponseCode(200);
        asserter.assertContentType("application/json; charset=UTF-8");
        
        Map json = asserter.getContentAsJsonMap();
        Assert.assertEquals(json.get("x"), 1);
        Assert.assertEquals(json.get("y"), "yy");
    }

    public void test404() {
        server.get("notfound/").assertResponseCode(404);
    }

}
