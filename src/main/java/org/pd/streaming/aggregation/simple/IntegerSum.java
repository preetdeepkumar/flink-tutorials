package org.pd.streaming.aggregation.simple;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerSum
{
	private static final Logger logger = LoggerFactory.getLogger(Integer.class);
	
	private StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    private IntegerGeneratorSource source = new IntegerGeneratorSource(500);
    
    public void init() throws Exception
    {
    	// add a simple integer generator as source 
    	DataStream<Integer> dataStream = senv.addSource(source);
        
        // build the pipeline using tumbling window size of 5 seconds
    	dataStream        
        .timeWindowAll(Time.seconds(10))        
        .apply(new AllWindowFunction<Integer,Integer,TimeWindow>()
        {
            private static final long serialVersionUID = -6868504589463321679L;

            // Sum all the integers within this timeWindow
            @Override
            public void apply( TimeWindow window, Iterable<Integer> values, Collector<Integer> out ) throws Exception
            {
            	LocalTime windowTime = Instant.ofEpochMilli(window.getStart()).atZone(ZoneId.systemDefault()).toLocalTime();
            	  
            	logger.info("Window starts at {} and called at {}", windowTime, LocalTime.now());
            	
            	Integer result = 0;
                
                for(Integer v : values)
                {
                    result += v;
                }
                
                out.collect( result );
            }
        })        
        .addSink( new SinkFunction<Integer>() 
        {
        	private static final long serialVersionUID = 4123288113227508752L;
        	
        	// computation done for this timeWindow, simply print the aggregated value
            @Override
            public void invoke(Integer value) 
            {
                logger.info("Sink called at {} with value {}", LocalTime.now(), value);
            }
        });

        senv.execute(this.getClass().getSimpleName());
    }
}
