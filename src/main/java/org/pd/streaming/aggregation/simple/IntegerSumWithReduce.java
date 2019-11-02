package org.pd.streaming.aggregation.simple;

import java.time.LocalTime;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author preetdeep.kumar
 */
public class IntegerSumWithReduce
{
	private static final Logger logger = LoggerFactory.getLogger(Integer.class);
	
	private StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    private IntegerGeneratorSource source = new IntegerGeneratorSource(500);
    
    public void init() throws Exception
    {
    	// add a simple integer generator as source 
    	DataStream<Integer> dataStream = senv.addSource(source);
        
        // build the pipeline using tumbling window size of 10 seconds
    	dataStream        
        .timeWindowAll(Time.seconds(10))        
        .reduce(new ReduceFunction<Integer>() 
        {
			private static final long serialVersionUID = -6449994291304410741L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception 
			{
				logger.info("Reduce called at {} with value1 {} and value2 {}", LocalTime.now(), value1, value2);
				return value1 + value2;
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
