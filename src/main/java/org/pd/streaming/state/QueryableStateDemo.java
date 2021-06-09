package org.pd.streaming.state;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

public class QueryableStateDemo
{

    public static void main(String[] args) throws Exception
    {
        Configuration config = new Configuration();
        // default values, but we can change them.
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // conecting to a socket with incoming data, like June, 1.
        // creo que quiere hacer la suma de todas las compras dada el mes...
        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>()
        {
            public Tuple2<Long, String> map(String s)
            {
                String[] words = s.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        })
                .keyBy(0)
                .flatMap(new StatefulMap());
        Random random = new Random();
        String outputPath = "file:///C://Users//ai.roman//queryable-state-out-"+random+".txt";
        sum.writeAsText(outputPath);

        // execute program
        env.executeAsync("State");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long>
    {
        private transient ValueState<Long> sum;            // 2
        private transient ValueState<Long> count;          //  4

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out)throws Exception
        {
            Long currCount = count.value();        //   2
            Long currSum = sum.value();             //  4

            currCount += 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if (currCount >= 10)
            {
                /* emit sum of last 10 elements */
                out.collect(sum.value());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }
        public void open(Configuration conf)
        {
            ValueStateDescriptor<Long> descriptor =new ValueStateDescriptor<Long>("sum", Long.class, 0L);
            descriptor.setQueryable("sum-query");
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>( "count",  Long.class, 0L);
            //descriptor2.setQueryable("count-query");
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}


