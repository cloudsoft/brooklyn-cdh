package io.cloudsoft.cloudera.rest;

import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.Method

import org.slf4j.Logger
import org.slf4j.LoggerFactory

public class RestCaller {

    private static final Logger log = LoggerFactory.getLogger(RestCaller.class);
    
    String urlBase;
    String authName, authPass;
    Object context = "Server";
    
    protected HTTPBuilder newHttpBuilder(String urlExtension) {
        def http = new HTTPBuilder( urlBase + urlExtension )
        if (authName!=null && authPass!=null) http.auth.basic authName, authPass;
        http.handler.failure = { resp, data ->
            log.warn "Unexpected failure: ${urlBase+urlExtension} (rethrowing)\n  ${resp.statusLine} ${resp.status} ${resp.data} ${resp.responseData}\n"+
                "  headers:"+resp.headers.collect().inject("", {r,i -> r+"  "+i })+"\n"+
                "  data:  ${data}"
            throw new IllegalStateException("${context} could not process request ("+resp.statusLine+")")
        }
        return http;
    }
    
    public Object doGet(Map fields=[:], String urlExtension) {
        log.debug "GET "+urlBase+urlExtension+" : "+fields
        Object result;
        Object o = newHttpBuilder(urlExtension).get(query: fields) { resp, data ->
            if (resp.status==200) result = data;
            else throw new IllegalStateException("${context} responded: "+resp)
        }
        return result
    }
    
    public Object doPost(Map props=[:], String urlExtension) {
        Map props2 = [:] + props
        props2.remove "body"
        def json = new groovy.json.JsonBuilder()
        json.call(props["body"])
        log.debug "POST "+urlBase+urlExtension+" : "+props2+" ("+Thread.currentThread()+")\n  "+
            json.toString()
        Object result;
        Object o = newHttpBuilder(urlExtension).post( props ) { resp, data ->
            if (resp.status==200) {
                log.debug("POST RESULT 200 - "+data+" ("+Thread.currentThread()+")")
                result = data;
            } else {
                // doesn't usu come here -- see failure handler in newHttpBuilder
                log.warn("POST RESULT "+resp.status+" - "+data+" ("+Thread.currentThread()+")")
                throw new IllegalStateException("${context} responded: "+resp)
            }
        }
    }

    public Object doPut(Map props=[:], String urlExtension) {
        log.debug "PUT "+urlBase+urlExtension+" : "+props
        Object result;
        Object contentType = props.remove("requestContentType");
        if (contentType==null) contentType = ContentType.JSON;
        Object o = newHttpBuilder(urlExtension).request(urlBase+urlExtension, Method.PUT, contentType) { req ->
            body = props.remove("body")
            if (props) log.warn("Ignoring properties in PUT call "+urlBase+urlExtension+": "+props);
            response.success = { resp, data ->
                if (resp.status==200) result = data;
                else throw new IllegalStateException("${context} responded: "+resp)
            }
        }
    }

}
