package io.cloudsoft.test.rest;

import java.util.Map;

public interface SimpleWebRequestProcessor {
    
    /** return type ignored, but included for convenient bail-out in 'if' clauses; see test case */
    public Object handle(String path, Map<String,String> query, SimpleWebResponder responder);
    
}
