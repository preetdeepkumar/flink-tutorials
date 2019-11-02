package org.pd.streaming.aggregation.key;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pd.streaming.aggregation.simple.IntegerSum;

/**
 * Optimized version of {@link IntegerSum} using Flink provided API
 * 
 * @author preetdeep.kumar
 */
public class IntegerSumWithKeyFromPojo
{
	private StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    
    public void init() throws Exception
    {
    	// add a source using simple POJO 
    	DataStream<MyData> dataStream = 
    			senv.addSource(new SourceFunction<MyData>() 
    						   {
    								private static final long serialVersionUID = -1356281802334002703L;
    								private String[] id = new String[] {"id-1", "id-2", "id-3"};
    								
									@Override
									public void run(SourceContext<MyData> ctx) throws Exception {
										int counter = 1;
										while(true) {
											Thread.sleep( 500 );
											ctx.collect(new MyData(id[counter%3], counter++));
										}
									}
							
									@Override
									public void cancel() {
										// production code must handle cancellation properly.
									}
							    });
        
        // build the pipeline using tumbling window size of 10 seconds
    	dataStream
    	.keyBy((KeySelector<MyData, String>) MyData::getId)
    	.timeWindowAll(Time.seconds(10))
    	.sum("value")
    	.print();

        senv.execute(this.getClass().getSimpleName());
    }
}
