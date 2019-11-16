package org.pd.streaming.event;

import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class EventSource implements SourceFunction<Tuple2<SensorEvent, Integer>>
{
    private static final long serialVersionUID = 2753805685983404539L;
    private volatile boolean isRunning = true;
    private String[] deviceID = new String[] { "id-1", "id-2", "id-3" };
    private String[] eventType = new String[] { "CONNECTED", "DISCONNECTED" };
    
    Random r = new Random(System.currentTimeMillis());
    
    @Override
    public void run( SourceContext<Tuple2<SensorEvent,Integer>> ctx ) throws Exception
    {
        SensorEvent randomEvent;
        
        while( isRunning )
        {
            Thread.sleep( 100 );
            
            for(int i=0; i<10; i++)
            {
                randomEvent = new SensorEvent( eventType[r.nextInt( 2 )], deviceID[r.nextInt( 3 )] );
                ctx.collect( new Tuple2<SensorEvent,Integer>( randomEvent, 0 ) );
            }
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}
