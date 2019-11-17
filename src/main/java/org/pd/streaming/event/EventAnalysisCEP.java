package org.pd.streaming.event;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * This example shows how can we processing connectivity events in Flink using CEP library.
 * 
 * Pattern - CONNECT and DISCONNECT events arrive in sequence for a device 2 times during 10 seconds 
 *  
 * @author preetdeep.kumar
 */
public class EventAnalysisCEP
{
    final int THRESHOLD = 2;

    @SuppressWarnings( "serial" )
    public void init() throws Exception
    {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorEvent> inputStream = senv
                                              .addSource( new EventSourceCEP() )
                                              .keyBy( (KeySelector<SensorEvent, String>) SensorEvent::getDeviceId);        

        SkipPastLastStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        
        Pattern<SensorEvent, ?> pattern = Pattern
                                          .<SensorEvent>begin("start", skipStrategy)
                                          .where( new SimpleCondition<SensorEvent>() 
                                          {
                                              @Override
                                              public boolean filter(SensorEvent event) 
                                              {
                                                  return event.isConnected();
                                              }
                                          })
                                          .next("end")
                                          .where( new SimpleCondition<SensorEvent>() 
                                          {
                                            @Override
                                            public boolean filter(SensorEvent event) 
                                            {
                                                return event.isDisconnected();
                                            }
                                          })
                                          .within( Time.seconds( 5 ) );

        PatternStream<SensorEvent> patternStream = CEP.pattern(inputStream, pattern);
        
        patternStream
        .process(new PatternProcessFunction<SensorEvent, String>()
        {
            int count = 0;
            
            @Override
            public void processMatch(Map<String, List<SensorEvent>> match,
                                     Context ctx,
                                     Collector<String> out) throws Exception 
            {
                count++;
                
                if(count > THRESHOLD)
                {
                    String message = "Pattern found for " + match.get( "start" ).get( 0 ).deviceId;
                    out.collect(message);
                    count = 0;
                }
            }
        })
        .print();
        
        senv.execute();
    }
}
