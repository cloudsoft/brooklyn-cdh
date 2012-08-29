package io.cloudsoft.test.rest;

import groovy.json.JsonSlurper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import org.testng.Assert;

import brooklyn.test.TestUtils;
import brooklyn.util.ResourceUtils;

import com.google.common.base.Throwables;

public class SimpleWebConnectionAsserter {

    private URLConnection connection;

    public SimpleWebConnectionAsserter(URLConnection connection) {
        this.connection = connection;
    }

    public SimpleWebConnectionAsserter(String url) {
        this(TestUtils.connectToURL(url));
    }

    public URLConnection getConnection() {
        return connection;
    }

    public HttpURLConnection getHttpConnection() {
        return (HttpURLConnection) connection;
    }

    public SimpleWebConnectionAsserter assertResponseCode(int code) {
        try {
            Assert.assertEquals(getHttpConnection().getResponseCode(), code);
            return this;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public SimpleWebConnectionAsserter assertContentType(String type) {
        Assert.assertEquals(getHttpConnection().getContentType(), type);
        return this;
    }

    public SimpleWebConnectionAsserter assertContentEquals(String exactMatch) {
        Assert.assertEquals(getContent(), exactMatch);
        return this;
    }

    public SimpleWebConnectionAsserter assertContentMatchesRegex(String regexMatch) {
        String content = getContent();
        Assert.assertTrue(getContent().matches(regexMatch), "Web content doesn't match regex '"+regexMatch+"'\nContent is:\n"+content);
        return this;
    }

    public SimpleWebConnectionAsserter assertContentContains(String phrase) {
        String content = getContent();
        Assert.assertTrue(content.indexOf(phrase)>=0, "Web content missing '"+phrase+"'\nContent is:\n"+content);
        return this;
    }

    public String getContent() {
        try {
            Object content = getHttpConnection().getContent();
            if (content==null) return null;
            if (content instanceof InputStream) content = ResourceUtils.readFullyString((InputStream)content);
            if (content==null || content instanceof String) return (String)content;
            return content.toString();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public Object getContentAsJson() {
        return new JsonSlurper().parseText(getContent());
    }

    @SuppressWarnings("rawtypes")
    public Map getContentAsJsonMap() {
        return (Map)getContentAsJson();
    }

    @SuppressWarnings("rawtypes")
    public List getContentAsJsonList() {
        return (List)getContentAsJson();
    }

}
