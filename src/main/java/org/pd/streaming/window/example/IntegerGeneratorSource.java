package org.pd.streaming.window.example;

import java.time.LocalTime;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple source class which produces an integer every second 
 * @author preetdeep.kumar
 */
@SuppressWarnings( "serial" )
class IntegerGenerator implements SourceFunction<Integer>
{
    volatile boolean isRunning = true;
    final Logger logger = LoggerFactory.getLogger(IntegerGenerator.class);
        
    @Override
    public void run( SourceContext<Integer> ctx ) throws Exception
    {
        int counter = 1;
        
        while( isRunning )
        {
            ctx.collect( counter );

            logger.info("Produced Integer value {} at {}", counter++, LocalTime.now());
            
            Thread.sleep( 1000 );
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}