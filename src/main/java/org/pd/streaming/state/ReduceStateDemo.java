package org.pd.streaming.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

public class ReduceStateDemo
{
    public static void main(String[] args) throws Exception
    {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                .flatMap(new MyStatefulMapWithReduce());
        Random random=new Random();
        String path = "file:///C://Users//ai.roman//ListStateDemo/state-with-reduce-" + random.nextInt();
        sum.writeAsText(path);

        // execute program. I run it async because maybe it will not the only program running in the cluster
        env.executeAsync("State");
    }

    public static class MyStatefulMapWithReduce extends RichFlatMapFunction<Tuple2<Long, String>, Long>
    {
        private transient ValueState<Long> count;
        private transient ReducingState<Long> sum;

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out)throws Exception
        {
            Long currValue = Long.parseLong(input.f1);
            Long currCount = count.value();

            currCount += 1;

            count.update(currCount);
            sum.add(currValue);

            if (currCount >= 10)
            {
                /* emit sum of last 10 elements */
                out.collect(sum.get());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }

        public void open(Configuration conf)
        {
            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>( "count",  Long.class, 0L);
            count = getRuntimeContext().getState(descriptor2);

            ReducingStateDescriptor<Long> sumDesc = new ReducingStateDescriptor<Long>("reducing sum", new SumReduce(), Long.class);
            sum = getRuntimeContext().getReducingState(sumDesc);
        }

        public class SumReduce implements ReduceFunction<Long>
        {
            public Long reduce(Long commlativesum, Long currentvalue)
            {
                return commlativesum+currentvalue;
            }
        }
    }
}


