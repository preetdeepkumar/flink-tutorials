package org.pd.streaming.aggregation.simple;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IntegerSum
{
    private StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    private IntegerGeneratorSource source = new IntegerGeneratorSource();
    
    public void init() throws Exception
    {
    	// add a simple integer generator as source 
    	DataStream<Integer> dataStream = senv.addSource(source);
        
        // build the pipeline using tumbling window size of 5 seconds
    	dataStream        
        .timeWindowAll(Time.seconds(5))        
        .apply(new AllWindowFunction<Integer,Integer,TimeWindow>()
        {
            private static final long serialVersionUID = -6868504589463321679L;

            // Sum all the integers within this timeWindow
            @Override
            public void apply( TimeWindow window, Iterable<Integer> values, Collector<Integer> out ) throws Exception
            {
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
                System.out.println(value);
            }
        });

        senv.execute(this.getClass().getSimpleName());
    }
}
