package org.pd.streaming.event;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * This example shows how can we processing connectivity events in Flink. It also uses
 * a simple pattern to identify and log an anomaly.
 * 
 * Pattern - CONNECT and DISCONNECT events arrive in sequence for a device 2 times during 10 seconds 
 *  
 * @author preetdeep.kumar
 */
public class EventAnalysis
{
    final int THRESHOLD = 2;
    
    @SuppressWarnings( "serial" )
    public void init() throws Exception
    {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<SensorEvent, Integer>> stream = senv.addSource( new EventSource() );
        
        stream
        .keyBy( new KeySelector<Tuple2<SensorEvent,Integer>, String>()
        {
            @Override
            public String getKey( Tuple2<SensorEvent,Integer> value ) throws Exception
            {
                return value.f0.getDeviceId();
            }
        })        
        .timeWindow( Time.seconds( 10 ) ) // 10 seconds window
        .reduce(new ReduceFunction<Tuple2<SensorEvent,Integer>>()
        {
            Tuple2<SensorEvent,Integer> tempResult = new Tuple2<SensorEvent,Integer>(new SensorEvent(), 0);
            
            @Override
            public Tuple2<SensorEvent,Integer> reduce( Tuple2<SensorEvent, Integer> prev,
                                                       Tuple2<SensorEvent, Integer> curr )
                throws Exception
            {
                tempResult.f0 = curr.f0;
                
                // find pattern
                if( prev.f0.isConnected() && curr.f0.isDisconnected())
                {
                    // pattern found, increment threshold of current event
                    tempResult.f0 = curr.f0;
                    tempResult.f1 = tempResult.f1 + 1;
                }
                
                return tempResult;
            }
        })
        .addSink( new SinkFunction<Tuple2<SensorEvent,Integer>>()
        {
            @Override
            public void invoke(Tuple2<SensorEvent,Integer> result) 
            {
                System.out.println("Device ID = " + result.f0.deviceId+ " Total pattern count = " + result.f1);
                
                if( result.f1 > THRESHOLD )
                {
                    System.out.println( "================================================================" );
                    System.out.println("Device ID = " + result.f0.deviceId+ " Total pattern count = " + result.f1);
                    System.out.println( "================================================================" );
                }
            }
        });
        
        senv.execute();
    }
}
