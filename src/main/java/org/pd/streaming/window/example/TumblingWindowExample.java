package org.pd.streaming.window.example;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example code for TumblingWindow to sum integers using a 
 * simple integer generator as source
 * 
 * @author preetdeep.kumar
 */
public class TumblingWindowExample
{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Integer> intStream = env.addSource( new IntegerGenerator() );
    
    static final Logger logger = LoggerFactory.getLogger(TumblingWindowExample.class);

    public static void main(String[] arg) throws Exception
    {
        new TumblingWindowExample().exampleTimeWindow();
        new TumblingWindowExample().exampleCountWindow();
    }
    
    @SuppressWarnings( "serial" )
    void exampleTimeWindow() throws Exception
    {
        intStream
        .timeWindowAll( Time.seconds( 5 ) ) // all integers within 5 second time window
        .process( new ProcessAllWindowFunction<Integer, Integer ,TimeWindow>()
        {
            @Override
            public void process( Context arg0, Iterable<Integer> input, Collector<Integer> output ) throws Exception
            {
                logger.info( "Computing sum for {}", input );
                
                int sum = 0;
                for(int i : input) {
                    sum += i;
                }
                output.collect( sum );
            }
        })
        .print();
        
        env.executeAsync("exampleTimeWindow");
    }
    
    @SuppressWarnings( "serial" )
    void exampleCountWindow() throws Exception
    {
        intStream
        .countWindowAll( 4 )
        .reduce( new ReduceFunction<Integer>()
        {
            @Override
            public Integer reduce( Integer value1, Integer value2 ) throws Exception
            {
                logger.info( "Reducing {} and {}", value1, value2 );
                return value1 + value2;
            }
        })                
        .print();
        
        env.executeAsync("exampleCountWindow");
    }
}
