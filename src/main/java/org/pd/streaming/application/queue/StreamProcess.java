package org.pd.streaming.application.queue;

import static org.pd.streaming.application.queue.Controller.QUEUE_NAME;

import java.time.Instant;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

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
    
    @SuppressWarnings( "serial" )
    @PostConstruct
    public void init()
    {
        try
        {
            Destination destination = mySession.createQueue(QUEUE_NAME);
            
            MessageConsumer consumer = mySession.createConsumer( destination );
            
            source = new AMQSource(consumer);
            
            DataStream<ApacheLogMessage> dataStream = env.addSource( source );
            
            dataStream
            .keyBy( (KeySelector<ApacheLogMessage, String>) ApacheLogMessage::getClientip )
            .timeWindow( Time.seconds( 10 ) )
            .apply( new WindowFunction<ApacheLogMessage,Tuple2<String, Long>, String,TimeWindow>()
            {
                @Override
                public void apply( String key, TimeWindow window, Iterable<ApacheLogMessage> input, 
                                   Collector<Tuple2<String,Long>> out ) throws Exception
                {
                    long count = 0;
                    for( ApacheLogMessage msg : input)
                    {
                        if ( HttpStatus.valueOf( msg.getResponse() ).is4xxClientError() )
                        {
                            count++;
                        }
                    }
                    out.collect( new Tuple2<>(key, count) );
                }
            })
            .filter( new FilterFunction<Tuple2<String,Long>>()
            {
                @Override
                public boolean filter( Tuple2<String,Long> value ) throws Exception
                {
                    return value.f1 > 0;
                }
            })
            .print();
            
            //System.out.println( env.getExecutionPlan() );
            
            startStreamEnvironment();
        }
        catch(JMSException e)
        {
            logger.error( "Failed to create Queue", e );
        }
    }

    private void startStreamEnvironment()
    {
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
    
    @PreDestroy
    public void stop()
    {
        if( source != null )
        {
            source.cancel();
        }
    }
}
