package org.pd.streaming.window.example;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TumblingWindowEventTime {

    static final Logger logger = LoggerFactory.getLogger(TumblingWindowEventTime.class);
    static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] arg) throws Exception {
        //new TumblingWindowEventTime().testTimestampsAndWatermarks();
        //new TumblingWindowEventTime().handleLateElements();
        new TumblingWindowEventTime().handleBranchingDataFlows();

    }

    private void testTimestampsAndWatermarks() throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Element> elementStream = env.addSource(new ElementGeneratorSource());

        elementStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Element>() {
                    @Override
                    public long extractAscendingTimestamp(Element element) {
                        return element.getTimestamp();
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Element, Integer, TimeWindow>() {
                    @Override
                    public void process(Context arg0, Iterable<Element> input, Collector<Integer> output) throws Exception {
                        logger.info("Computing sum for {}", input);
                        int sum = 0;
                        for (Element e : input) {
                            sum += e.getValue();
                        }
                        output.collect(sum);
                    }
                })
                .print();

        env.executeAsync("testTimestampsAndWatermarks");
    }

    private void handleLateElements() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Element> elementStream = env.addSource(new ElementGeneratorSource());
        elementStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Element>() {
                    @Override
                    public long extractAscendingTimestamp(Element element) {
                        return element.getTimestamp();
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .process(new ProcessAllWindowFunction<Element, Integer, TimeWindow>() {
                    @Override
                    public void process(Context arg0, Iterable<Element> input, Collector<Integer> output) throws Exception {
                        logger.info("Computing sum for {}", input);
                        int sum = 0;
                        for (Element e : input) {
                            sum += e.getValue();
                        }
                        output.collect(sum);
                    }
                })
                .print();
        env.executeAsync("handleLateElements");
    }

// https://dzone.com/articles/deep-dive-into-apache-flinks-tumblingwindow-part-3
    private void handleBranchingDataFlows() throws Exception {

        final OutputTag<Element> outputLateTag = new OutputTag<Element>("side-output-late") {
        };
        final OutputTag<Element> outputErrTag = new OutputTag<Element>("side-output-error") {
        };

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Element> elementStream = env.addSource(new ElementGeneratorSource());

        SingleOutputStreamOperator<Integer> stream =
                elementStream
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Element>() {
                            @Override
                            public long extractAscendingTimestamp(Element element) {
                                return element.getTimestamp();
                            }
                        })
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .sideOutputLateData(outputLateTag)
                        .process(new ProcessAllWindowFunction<Element, Integer, TimeWindow>() {
                            @Override
                            public void process(Context ctx, Iterable<Element> input, Collector<Integer> output) throws Exception {
                                logger.info("Computing sum for {}", input);
                                int sum = 0;
                                for (Element e : input) {
                                    sum += e.getValue();
                                    // send to a side stream
                                    ctx.output(outputErrTag, e);

                                }
                                output.collect(sum);

                            }

                        });
        // get late and error streams as side output
        DataStream<Element> lateStream = stream.getSideOutput(outputLateTag);
        DataStream<Element> errStream = stream.getSideOutput(outputErrTag);

// print to console
        logger.info("normal_stream:");
        stream.print();
        logger.info("late_stream:");
        lateStream.print();
        logger.info(" error_stream:");
        errStream.print();
        env.executeAsync("handleBranchingDataFlows");
    }
}
