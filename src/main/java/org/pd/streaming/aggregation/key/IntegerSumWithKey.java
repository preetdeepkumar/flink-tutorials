package org.pd.streaming.aggregation.key;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pd.streaming.aggregation.simple.IntegerSum;

/**
 * Optimized version of {@link IntegerSum} using Flink provided API
 * 
 * @author preetdeep.kumar
 */
public class IntegerSumWithKey
{
	private StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    private IntegerGeneratorSourceWithKey source = new IntegerGeneratorSourceWithKey(500);
    
    public void init() throws Exception
    {
    	// add a simple integer generator as source 
    	DataStream<Tuple2<String, Integer>> dataStream = senv.addSource(source);
        
        // build the pipeline using tumbling window size of 10 seconds
    	dataStream
    	.keyBy(0)
    	.timeWindowAll(Time.seconds(10))
    	.sum(1)
    	.print();

        senv.execute(this.getClass().getSimpleName());
    }
}
