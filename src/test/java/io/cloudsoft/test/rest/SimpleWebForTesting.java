package io.cloudsoft.test.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;

import com.google.common.base.Throwables;

public class SimpleWebForTesting implements Container {

    protected SimpleWebRequestProcessor processor;

    public SimpleWebForTesting(SimpleWebRequestProcessor processor) {
        this.processor = processor;
    }
    
    public void handle(Request request, Response response) {
        SimpleWebResponder r = new SimpleWebResponder(request, response);
        
        String path = request.getPath().getPath();
        if (path.startsWith("/")) path = path.substring(1);
        if (path.endsWith("/")) path = path.substring(0, path.length()-1);
        
        processor.handle(path, request.getQuery(), r);
        r.close();
    }

    private String url;
    private int port;
    private Connection connection;

    public int getPort() {
        return port;
    }

    /** URL, including trailing slash */
    public String getUrl() {
        return url;
    }

    public synchronized void close() {
        try {
            if (connection==null) return;
            connection.close();
            port = -1;
            connection = null;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }        

    @SuppressWarnings("resource")
    public static SimpleWebForTesting launch(SimpleWebRequestProcessor processor) {
        SimpleWebForTesting server = new SimpleWebForTesting(processor);
        Connection connection;
        try {
            connection = new SocketConnection(server);
        } catch (IOException e1) {
            throw Throwables.propagate(e1);
        }
        int port = 18001;
        Throwable e = null;
        while (port<19000) {
            try {
                SocketAddress address = new InetSocketAddress(port);
                connection.connect(address);
                server.connection = connection;
                server.port = port;
                server.url = "http://localhost:"+port+"/";
                return server;
            } catch (Throwable e2) {
                e = e2;
            }
        }
        throw new IllegalStateException("No available ports for launching server: "+e);
    }

    public SimpleWebConnectionAsserter get(String urlExtension) {
        return new SimpleWebConnectionAsserter(getUrl()+urlExtension);
    }
    
    @Override
    public String toString() {
        return "SimpleWebForTesting["+processor+"@port"+port+"]";
    }
}