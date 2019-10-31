package org.pd.streaming.aggregation.simple;

import java.time.LocalTime;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegerGeneratorSource implements SourceFunction<Integer>
{
    private static final long serialVersionUID = -9049744310869990871L;
    private volatile boolean isRunning = true;
    private long sleepTimer;
    private static final Logger logger = LoggerFactory.getLogger(IntegerGeneratorSource.class);
        
    public IntegerGeneratorSource()
    {
    	this(100);
    }
    
    public IntegerGeneratorSource(long sleepTimer)
    {
    	this.sleepTimer = sleepTimer;
    }

    @Override
    public void run( SourceContext<Integer> ctx ) throws Exception
    {
        int counter = 1;
        while( isRunning )
        {
            // by default emit 10 integers per second
            Thread.sleep( sleepTimer );
            ctx.collect( counter++ );
            logger.info("Emitted at {}", LocalTime.now());
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}