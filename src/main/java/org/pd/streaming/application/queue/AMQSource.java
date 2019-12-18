package org.pd.streaming.application.queue;

import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Simple Flink Source class to read messages from ActiveMQ
 * 
 * @author preetdeep.kumar
 */
public class AMQSource extends RichSourceFunction<String>
{
    private static final long serialVersionUID = 2609359039011604917L;
    
    private volatile boolean running = true;
    
    private static MessageConsumer consumer;

    public AMQSource(MessageConsumer c)
    {
        consumer = c;
    }

    @Override
    public void open(Configuration parameters) throws Exception 
    {
        super.open(parameters);
    }
    
    @Override
    public void run( SourceContext<String> ctx ) throws Exception
    {
        while( running )
        {
            Message message = consumer.receive();
            ctx.collect( message.getBody( String.class ) );
        }
    }

    @Override
    public void cancel()
    {
        running = false;
    }
}
