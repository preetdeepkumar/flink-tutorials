package org.pd.streaming.window.example;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TumblingWindowEventTime {

    static final Logger logger = LoggerFactory.getLogger(TumblingWindowEventTime.class);
    static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] arg) throws Exception
    {
        new TumblingWindowEventTime().testTimestampsAndWatermarks();

    }

    private void testTimestampsAndWatermarks() throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Element> elementStream = env.addSource( new ElementGeneratorSource() );

        elementStream
                .assignTimestampsAndWatermarks( new AscendingTimestampExtractor<Element>(){
                    @Override
                    public long extractAscendingTimestamp( Element element )
                    {
                        return element.getTimestamp();
                    }
                })
                .windowAll( TumblingEventTimeWindows.of( Time.seconds( 10 ) ) )
                .process( new ProcessAllWindowFunction<Element, Integer , TimeWindow>(){
                    @Override
                    public void process( Context arg0, Iterable<Element> input, Collector<Integer> output ) throws Exception
                    {
                        logger.info( "Computing sum for {}", input );
                        int sum = 0;
                        for(Element e : input) {
                            sum += e.getValue();
                        }
                        output.collect( sum );
                    }
                })
                .print();

        env.execute();
    }
}
