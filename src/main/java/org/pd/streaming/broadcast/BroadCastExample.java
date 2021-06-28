package org.pd.streaming.broadcast;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Random;

public class BroadCastExample
{
    public static final MapStateDescriptor<String, String> excludeEmpDescriptor =
            new MapStateDescriptor<String, String>("exclude_employ", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> excludeEmp =	env.socketTextStream("localhost", 9090);

        BroadcastStream<String> excludeEmpBroadcast =	excludeEmp.broadcast(excludeEmpDescriptor);

        String path = "file:///C://Users//ai.roman//broadcast.txt";
        // ojito, parece que si le paso una ruta interna al jar no funciona... /src/main/resources/broadcast.txt
        // confirmado. Tiene que leer de alguna parte externa, del HDFS, o de un FS normal con rutas absolutas.
        DataStream<Tuple2<String, Integer>> employees = env.readTextFile(path)
                .map(new MapFunction<String, Tuple2<String, String>>()
                {
                    public Tuple2<String, String> map(String value)
                    {
                        // dept, data
                        return new Tuple2<String, String>(value.split(",")[3], value);   // {(Purchase), (AXPM175755,Nana,Developer,Purchase,GH67D)}
                    }
                })
                .keyBy(0)
                .connect(excludeEmpBroadcast)  // will return a BroadcastConnectedStream
                .process(new ExcludeEmp());
        //employees.print();
        Random random = new Random();
        String outputPath = "file:///C://Users//ai.roman//broadcast-out-"+random+".txt";
        employees.writeAsText(outputPath);
        env.executeAsync("Broadcast Exmaple");
        System.out.println("Done!. A file was created at " + outputPath);
    }

    public static class ExcludeEmp extends KeyedBroadcastProcessFunction<String,Tuple2<String, String>, String, Tuple2<String, Integer>>
    {
        private transient ValueState<Integer> countState;

        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out)throws Exception
        {
            Integer currCount = countState.value();
            // get card_id of current transaction
            final String cId = value.f1.split(",")[0];

            for (Map.Entry<String, String> cardEntry: ctx.getBroadcastState(excludeEmpDescriptor).immutableEntries())
            {
                final String excludeId = cardEntry.getKey();
                if (cId.equals(excludeId))
                    return;
            }

            countState.update(currCount+1);       // dept    , current sum
            out.collect(new Tuple2<String, Integer>(value.f0, currCount+1));
        }

        public void processBroadcastElement(String empData, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            String id = empData.split(",")[0];
            ctx.getBroadcastState(excludeEmpDescriptor).put(id, empData);
        }

        public void open(Configuration conf)
        {
            ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<Integer>("", BasicTypeInfo.INT_TYPE_INFO, 0);
            countState = getRuntimeContext().getState(desc);
        }
    }
}


