package org.pd.streaming.application.queue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Listen for log messages on http://<host-name>:8080/logs and send it to queue.
 * Spring Boot does the conversion of the JSON string payload to Java Object.
 * 
 * @author preetdeep.kumar
 *
 */
@RestController
@RequestMapping("/logs")
public class Controller
{
    @Autowired 
    private JmsTemplate jmsTemplate;
    
    public static final String QUEUE_NAME = "webserverlog";
    
    @PostMapping
    public ResponseEntity<?> sendToQueue(@RequestBody ApacheLogMessage message) 
    {
        jmsTemplate.convertAndSend(QUEUE_NAME, message );
        
        return new ResponseEntity<>(HttpStatus.ACCEPTED);            
    }
}
