package org.pd.streaming.application.queue;

import java.time.Instant;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import static org.pd.streaming.application.queue.Controller.*;

/**
 * This example shows how to read from activeMQ queue and pass it to Flink. This allows
 * for decoupling between producer and consumer.
 * 
 * Here I am using Spring Boot's default in-memory configuration to run ActiveMQ. 
 * 
 * @author preetdeep.kumar
 */
@Component
public class StreamProcess
{
    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    
    @Autowired
    private TaskScheduler scheduler;
    
    @Autowired
    public Session mySession;

    AMQSource source;
    
    @PostConstruct
    public void init()
    {
        try
        {
            Destination destination = mySession.createQueue(QUEUE_NAME);
            
            MessageConsumer consumer = mySession.createConsumer( destination );
            
            source = new AMQSource(consumer);
            
            DataStream<String> dataStream = env.addSource( source );
            
            //TODO - Add processing
            dataStream.print();
            
            scheduler.schedule( () -> 
            { 
                try 
                { 
                    env.execute(); 
                } 
                catch( Exception e) 
                { 
                    logger.error( "Cannot start Flink execution environment", e ); 
                } 
            }, Instant.now() );
             
        }
        catch(JMSException e)
        {
            logger.error( "Failed to create Queue", e );
        }
    }
    
    @PreDestroy
    public void stop()
    {
        if( source != null )
        {
            source.cancel();
        }
    }
}
