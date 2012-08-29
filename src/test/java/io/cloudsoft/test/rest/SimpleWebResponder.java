package io.cloudsoft.test.rest;

import groovy.json.JsonBuilder;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;

import com.google.common.base.Throwables;

public class SimpleWebResponder {
    Request request;
    Response response;
    PrintStream body;
    
    public SimpleWebResponder(Request request, Response response) {
        this.request = request;
        this.response = response;
        try {
            body = response.getPrintStream();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }                
    }

    public SimpleWebResponder useStandardHeaders(Response response) {
        response.set("Server", "SimpleWebForTesting/1.0");
        
        long time = System.currentTimeMillis();
        response.setDate("Date", time);
        response.setDate("Last-Modified", time);
        return this;
    }

    public SimpleWebResponder code(int code) {
        response.setCode(code);
        return this;
    }

    public SimpleWebResponder text(String text) {
        contentType("text/plain");
        body.print(text);
        return this;
    }

    public SimpleWebResponder html(String html) {
        contentType("text/html");
        body.print(html);
        return this;
    }

    @SuppressWarnings("rawtypes")
    public SimpleWebResponder json(Map map) {
        contentType("application/json; charset=UTF-8");
        JsonBuilder jb = new JsonBuilder();
        jb.call(map);
        body.print(jb.toString());
        return this;
    }

    public SimpleWebResponder contentType(String type) {
        response.set("Content-Type", type);
        return this;
    }
    
    public synchronized void close() {
        if (body==null) return;
        body.close();
        body = null;
    }
}