package org.pd.streaming.aggregation.simple;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

class IntegerGeneratorSource implements SourceFunction<Integer>
{
    private static final long serialVersionUID = -9049744310869990871L;
    private volatile boolean isRunning = true;
    private long sleepTimer;
    
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
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}