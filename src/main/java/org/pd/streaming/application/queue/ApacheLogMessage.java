package org.pd.streaming.application.queue;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * POJO for apache server log messages.
 * 
 * @author preetdeep.kumar
 */
@Getter @Setter @NoArgsConstructor @ToString
public class ApacheLogMessage implements Serializable
{
    private static final long serialVersionUID = -1267462536005102632L;
    
    String ident;
    String auth;
    String message;    
    Integer response;
    String timestamp;
    String referrer;
    String agent;
    String verb;
    String request;
    Integer bytes;
    String clientip;
}
