package org.pd.streaming.event;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings( "serial" )
public class EventSourceCEP implements SourceFunction<SensorEvent>
{
    private volatile boolean isRunning = true;
    private String[] deviceID = new String[] { "id-1", "id-2", "id-3" };
    private String[] eventType = new String[] { "CONNECTED", "DISCONNECTED" };
    
    Random r = new Random(System.currentTimeMillis());
    
    @Override
    public void run( SourceContext<SensorEvent> ctx ) throws Exception
    {
        SensorEvent randomEvent;
        
        while( isRunning )
        {
            Thread.sleep( 500 );
            
            for(int i=0; i<10; i++)
            {
                randomEvent = new SensorEvent( eventType[r.nextInt( 2 )], deviceID[r.nextInt( 3 )] );
                ctx.collect( randomEvent );
            }
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}
