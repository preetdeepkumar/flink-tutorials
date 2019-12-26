package org.pd.streaming.application.queue;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Simple Source class to read messages from ActiveMQ queue, cast to POJO and emit to Flink
 * 
 * @author preetdeep.kumar
 */
public class AMQSource extends RichSourceFunction<ApacheLogMessage>
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
    public void run( SourceContext<ApacheLogMessage> ctx ) throws Exception
    {
        while( running )
        {
            Message m = consumer.receive();
            
            ApacheLogMessage logMessage = (ApacheLogMessage)((ObjectMessage)m).getObject();
            
            ctx.collect( logMessage );
        }
    }

    @Override
    public void cancel()
    {
        running = false;
    }
}
