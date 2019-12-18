package org.pd.streaming.application.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/logs")
public class Controller
{
    @Autowired 
    private JmsTemplate jmsTemplate;
    
    public static final String QUEUE_NAME = "webserverlog";
    
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    
    @PostMapping
    public void sendToQueue(@RequestBody String message) 
    {
        jmsTemplate.convertAndSend(QUEUE_NAME, message );
        
        logger.info("Message sent {}", message);
    }
}
