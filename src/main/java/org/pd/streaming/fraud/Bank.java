package org.pd.streaming.fraud;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

/***
 * Super simple task to demonstrate i can flag suspicious events as fraudulents.
 * Task 47 of https://www.udemy.com/course/apache-flink-a-real-time-hands-on-course-on-flink/learn/lecture/12106448#overview
 *
 * The requirements are codified following these principles:
 *
 * 1) check if the customer in transaction is already alarmed in "alarmed_customers" data.
 * 2) check if the card is mentioned as lost in "lost_cards" data.
 * 3) Number of transactions are more than 10 in a minute.
 * 4) City of transaction is changed more than 2 times in 1 minute of time window.
 *
 * There is a process named BankDataServer that simulates the push process that flink needs to initialize itself with
 * incoming data. I copy a functional set of files in src/main/resources/data_fraud_flink_process folder.
 *
 */
public class Bank
{
    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCustStateDescriptor =
            new MapStateDescriptor<String, AlarmedCustomer>("alarmed_customers", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(AlarmedCustomer.class));

    public static final MapStateDescriptor<String, LostCard> lostCardStateDescriptor =
            new MapStateDescriptor<String, LostCard>("lost_cards",	BasicTypeInfo.STRING_TYPE_INFO,	Types.POJO(LostCard.class));

    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // These set of files MUST REMAIN in a distributed environment like HDFS, S3, or whatever, because they are potentially huge
        String path_alarmed_cust = "file:///C://data_fraud_flink_process//alarmed_cust.txt";

        DataStream<AlarmedCustomer> alarmedCustomers =	 env.readTextFile(path_alarmed_cust)
                .map(new MapFunction<String, AlarmedCustomer>()
                {
                    public AlarmedCustomer map(String value)
                    {
                        return new AlarmedCustomer(value);
                    }
                });
        // broadcast alarmed customer data
        BroadcastStream<AlarmedCustomer> alarmedCustBroadcast =	 alarmedCustomers.broadcast(alarmedCustStateDescriptor);

        String path_lost_cards = "file:///C://data_fraud_flink_process//lost_cards.txt";
        DataStream<LostCard> lostCards = env.readTextFile(path_lost_cards)
                .map(new MapFunction<String, LostCard>()
                {
                    public LostCard map(String value)
                    {
                        return new LostCard(value);
                    }
                });

        // broadcast lost card data
        BroadcastStream<LostCard> lostCardBroadcast = lostCards.broadcast(lostCardStateDescriptor);

        // transaction data keyed by customer_id
        DataStream<Tuple2<String, String>> data = env.socketTextStream("localhost", 9090)
                .map(new MapFunction<String, Tuple2<String, String>>()
                {
                    public Tuple2<String, String> map(String value)
                    {
                        String[] words = value.split(",");

                        return new Tuple2<String, String>(words[3], value); //{(id_347hfx) (HFXR347924,2018-06-14 23:32:23,Chandigarh,id_347hfx,hf98678167,123302773033,774
                    }
                });

        // (1) Check against alarmed customers
        DataStream<Tuple2<String, String>> alarmedCustTransactions = data
                .keyBy(0)
                .connect(alarmedCustBroadcast)
                .process(new AlarmedCustCheck());

        // (2) Check against lost cards
        DataStream<Tuple2<String, String>> lostCardTransactions = data
                .keyBy(0)
                .connect(lostCardBroadcast)
                .process(new LostCardCheck());

        DataStream<Tuple2<String, String>> excessiveTransactions = data
                // (3) More than 10 transactions check
                .map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>()
                {
                    public Tuple3<String, String, Integer> map(Tuple2<String, String> value)
                    {
                        return new Tuple3<String, String, Integer>(value.f0, value.f1, 1);
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(2)
                .flatMap(new FilterAndMapMoreThan10());

        DataStream<Tuple2<String, String>> freqCityChangeTransactions = data
                //.keyBy(t -> t.f0)
                .keyBy(new KeySelector<Tuple2<String, String>, String>()
                {
                    public String getKey(Tuple2<String, String> value)
                    {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new Citychange());

        DataStream<Tuple2<String, String>> AllFlaggedTxn =
                alarmedCustTransactions	.union(lostCardTransactions, excessiveTransactions,	freqCityChangeTransactions);

        String flagged_transaction_path = "file:///C://data_fraud_flink_process//flagged_transaction.txt";
        AllFlaggedTxn.writeAsText(flagged_transaction_path);
        // execute program
        env.executeAsync("Streaming Bank");
        System.out.println("Done. A file was created at " + flagged_transaction_path);
    }

    public static class Citychange extends ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>
    {
        public void process(String key,	Context context,Iterable<Tuple2<String, String>> input,	Collector<Tuple2<String, String>> out)
        {
            String lastCity = "";
            int changeCount = 0;
            for (Tuple2<String, String> element : input)
            {
                String city = element.f1.split(",")[2].toLowerCase();

                if (lastCity.isEmpty())
                {
                    lastCity = city;
                } else
                {
                    if (!city.equals(lastCity))
                    {
                        lastCity = city;
                        changeCount += 1;
                    }	}

                if (changeCount >= 2)
                {
                    out.collect(new Tuple2<String, String>("__ALARM__", element + "marked for FREQUENT city changes"));
                }	 }
        }    }

    public static class FilterAndMapMoreThan10	implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple2<String, String>>
    {
        public void flatMap(Tuple3<String, String, Integer> value,Collector<Tuple2<String, String>> out)
        {
            // Are there more than 10 transactions in 1 minute or less?
            if (value.f2 > 10)
            {
                out.collect(new Tuple2<String, String>("__ALARM__", value + " marked for >10 TXNs"));
            }
        }    }

    public static class AlarmedCustCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer,
            Tuple2<String, String>>
    {

        public void processElement(Tuple2<String, String> value,ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception
        {
            for (Map.Entry<String, AlarmedCustomer> custEntry: ctx.getBroadcastState(alarmedCustStateDescriptor).immutableEntries())
            {
                final String alarmedCustId = custEntry.getKey();
                final AlarmedCustomer cust = custEntry.getValue();

                // get customer_id of current transaction
                final String tId = value.f1.split(",")[3];
                if (tId.equals(alarmedCustId))
                {
                    out.collect(new Tuple2<String, String>("____ALARM___","Transaction: " + value + " is by an ALARMED customer"));
                }
            }	}

        public void processBroadcastElement(AlarmedCustomer cust, Context ctx,Collector<Tuple2<String, String>> out)throws Exception
        {
            ctx.getBroadcastState(alarmedCustStateDescriptor).put(cust.id, cust);
        }    }

    public static class LostCardCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard, Tuple2<String, String>>
    {
        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out)throws Exception
        {
            for (Map.Entry<String, LostCard> cardEntry: ctx.getBroadcastState(lostCardStateDescriptor).immutableEntries())
            {
                final String lostCardId = cardEntry.getKey();
                final LostCard card = cardEntry.getValue();

                // get card_id of current transaction
                final String cId = value.f1.split(",")[5];
                if (cId.equals(lostCardId))
                {
                    out.collect(new Tuple2<String, String>("__ALARM__",	"Transaction: " + value + " issued via LOST card"));
                }
            }	}

        public void processBroadcastElement(LostCard card, Context ctx,	Collector<Tuple2<String, String>> out)	 throws Exception
        {
            ctx.getBroadcastState(lostCardStateDescriptor).put(card.id, card);
        }    }
}

