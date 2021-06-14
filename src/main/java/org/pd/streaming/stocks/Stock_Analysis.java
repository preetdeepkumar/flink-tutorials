package org.pd.streaming.stocks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Random;

/***
 * For every minute, calculate:
 *
 * a) Maximum trade price
 * b) Minimum trade price
 * c) Maximum trade volume
 * d) Minimum trade volume
 * e) % change in Max_Trade_price from previous 1 minute.
 * f) % change in MAx_Trade_volume from previous 1 minute.
 *
 * Ist report looks like this:
 *
 * From Timestamp           End Timestamp               Current Wdw   Current Wdw     % change in     Current Wndw    Current Wndw    %change in
 *                                                      MAx price     Min_price       Max Price       MAx Volume      Min Volume      MAx Vol
 *
 * 06/10/2010:08:00:00      06/10/2010:08:00:58:00      107.0         101.5           0.00              354881          330164          0.00
 * 06/10/2010:08:01:03      06/10/2010:08:00:59:00      103.0         101.0           -3.74             354948          330514          0.02
 * ...
 *
 * 2nd Report (Alert Report)
 *
 * -- For every 5 minute window, if the MAx_trade_price is changing (up or down) by more than 5% then record that event.
 *
 */
public class Stock_Analysis
{
    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // attention, i have added the header to this file!
        String path_futures_trades = "file:///C://stocks_data//FUTURES_TRADES.txt";

        DataStream<Tuple5<String, String, String, Double, Integer>> data = env.readTextFile(path_futures_trades)

                .map(new MapFunction<String, Tuple5<String, String, String, Double, Integer>>()
                {
                    public Tuple5<String, String, String, Double, Integer> map(String value)
                    {
                        // not processing header
                        if (value.startsWith("#")) return null;

                        String[] words = value.split(",");
                        // date,    time,     Name,       trade,                      volume
                        return new Tuple5<String, String, String, Double, Integer>(words[0], words[1], "XYZ", Double.parseDouble(words[2]), Integer.parseInt(words[3]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<String, String, String, Double, Integer>>()
                {
                    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

                    public long extractAscendingTimestamp(Tuple5<String, String, String, Double, Integer> value)
                    {
                        try
                        {
                            Timestamp ts = new Timestamp(sdf.parse(value.f0 + " " + value.f1).getTime());

                            return ts.getTime();
                        }
                        catch(Exception e)
                        {
                            throw new java.lang.RuntimeException("Parsing Error");
                        }
                    }
                });

        // Compute per window statistics
        DataStream<String> change = data .keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
        {
            public String getKey(Tuple5<String, String, String, Double, Integer> value)
            {
                return value.f2;
            }
        })
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new TrackChange());
        Random random = new Random();
        int nextAleatorial = random.nextInt();
        String path_Ist_report = "file:///C://stocks_data//Ist-report-"+nextAleatorial+".txt";
        change.writeAsText(path_Ist_report);

        // Alert when price change from one window to another is more than threshold
        DataStream<String> largeDelta = data .keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
        {
            public String getKey(Tuple5<String, String, String, Double, Integer> value)
            {
                return value.f2;
            }
        })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new TrackLargeDelta(5));
        String path_Alert_report = "file:///C://stocks_data//Alert-"+nextAleatorial+".txt";
        largeDelta.writeAsText(path_Alert_report);

        env.executeAsync("Stock Analysis");

        System.out.println("Done!. A file was created at " + path_Ist_report + " and " + path_Alert_report);

    }

    public static class TrackChange extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow>
    {
        private transient ValueState<Double> prevWindowMaxTrade;
        private transient ValueState<Integer> prevWindowMaxVol;

        public void process(String key,Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input,Collector<String> out)throws Exception
        {
            String windowStart = "";
            String windowEnd = "";
            Double windowMaxTrade = 0.0;              // 0
            Double windowMinTrade = 0.0;              // 106
            Integer windowMaxVol = 0;
            Integer windowMinVol = 0;                 // 348746

            for (Tuple5<String, String, String, Double, Integer> element : input)
            //  06/10/2010, 08:00:00, 106.0, 348746
            //  06/10/2010, 08:00:00, 105.0, 331580
            {
                if (windowStart.isEmpty())
                {
                    windowStart = element.f0 + ":" + element.f1;   // 06/10/2010 : 08:00:00
                    windowMinTrade = element.f3;
                    windowMinVol = element.f4;
                }
                if (element.f3 > windowMaxTrade)
                    windowMaxTrade = element.f3;

                if (element.f3 < windowMinTrade)
                    windowMinTrade = element.f3;

                if (element.f4 > windowMaxVol)
                    windowMaxVol = element.f4;
                if (element.f4 < windowMinVol)
                    windowMinVol = element.f4;

                windowEnd = element.f0 + ":" + element.f1;
            }

            Double maxTradeChange = 0.0;
            Double maxVolChange = 0.0;

            if (prevWindowMaxTrade.value() != 0)
            {
                maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
            }
            if (prevWindowMaxVol.value() != 0)
                maxVolChange = ((windowMaxVol - prevWindowMaxVol.value())*1.0 / prevWindowMaxVol.value()) * 100;

            out.collect(windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " + String.format("%.2f", maxTradeChange)
                    + ", " +	 windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange));

            prevWindowMaxTrade.update(windowMaxTrade);
            prevWindowMaxVol.update(windowMaxVol);
        }

        public void open(Configuration config)
        {
            prevWindowMaxTrade = getRuntimeContext().getState(new ValueStateDescriptor<Double>("prev_max_trade",BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0));

            prevWindowMaxVol = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("prev_max_vol",BasicTypeInfo.INT_TYPE_INFO, 0));
        }    }

    public static class TrackLargeDelta extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow>
    {
        private final double threshold;
        private transient ValueState<Double> prevWindowMaxTrade;

        public TrackLargeDelta(double threshold)
        {
            this.threshold = threshold;
        }

        public void process(String key,Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input,Collector<String> out)throws Exception
        {
            Double prevMax = prevWindowMaxTrade.value();
            Double currMax = 0.0;
            String currMaxTimeStamp = "";

            for (Tuple5<String, String, String, Double, Integer> element : input)
            {
                if (element.f3 > currMax)
                {
                    currMax = element.f3;
                    currMaxTimeStamp = element.f0 + ":" + element.f1;
                }}

            // check if change is more than specified threshold
            Double maxTradePriceChange = ((currMax - prevMax)/prevMax)*100;

            if ( prevMax != 0 &&  // don't calculate delta the first time
                    Math.abs((currMax - prevMax)/prevMax)*100 > threshold)
            {
                out.collect("Large Change Detected of " + String.format("%.2f", maxTradePriceChange) + "%" + " (" + prevMax + " - " + currMax + ") at  " + currMaxTimeStamp);
            }
            prevWindowMaxTrade.update(currMax);
        }

        public void open(Configuration config)
        {
            ValueStateDescriptor<Double> descriptor =  new ValueStateDescriptor<Double>( "prev_max",BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);
            prevWindowMaxTrade = getRuntimeContext().getState(descriptor);
        }    }
}
