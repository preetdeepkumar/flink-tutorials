package org.pd.streaming.aggregation.simple;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Generate integers with an id. Each record which is sent to stream is a Tuple2
 * of id and Integer. Flink 1.9 provides TupleXX classes where 'XX' is any number between 2 to 25. 
 */
class IntegerGeneratorSourceWithKey implements SourceFunction<Tuple2<String, Integer>>
{
    private static final long serialVersionUID = -9049744310869990871L;
    private volatile boolean isRunning = true;
    private long sleepTimer;
	private String[] id = new String[] {"id-1", "id-2", "id-3"};
        
    public IntegerGeneratorSourceWithKey(long sleepTimer)
    {    	
    	this.sleepTimer = sleepTimer;
    }
    
    @Override
    public void run( SourceContext<Tuple2<String, Integer>> ctx ) throws Exception
    {
        int counter = 1;
        while( isRunning )
        {
        	Thread.sleep( sleepTimer );
        	// generate integers with an id
            ctx.collect( new Tuple2<String, Integer>(id[counter%3],counter++));            
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}