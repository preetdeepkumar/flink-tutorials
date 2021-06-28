package org.pd.streaming.state;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

/***
 * This is only going to work in a real flink cluster, not in local mode...
 * It is going to connect to a Flink cluster, create a connection with a process, see QueryableStateDemo.java and
 * bring back some live results.
 */
public class GetQueryableState {

    public static void main(String[] args) throws Exception
    {
        // connecting to flink cluster through its proxy...
        QueryableStateClient client = new QueryableStateClient("192.168.11.1", 9069);
        // check open method in QueryableStateDemo...
        ValueStateDescriptor<Long> descriptor =new ValueStateDescriptor<Long>("sum", Long.class, 0L);

        Long key =1L;

        JobID jobId = new JobID();
        // taken from ui. This hurts.
        jobId.fromHexString("5906ea8d8940842f9e05aedddc8517ea");
        // check open method in QueryableStateDemo...
        CompletableFuture<ValueState<Long>> resultFuture =
                client.getKvState(jobId, "sum-query", key, BasicTypeInfo.LONG_TYPE_INFO, descriptor);

        // now handle the returned value
        resultFuture.thenAccept(response ->
        {
            try {
                Long res = response.value();

                System.out.println("Queried sum value: " + res);

            } catch (Exception e)
            {
                e.printStackTrace();
            }
            System.out.println("Exiting future ...");
        });
        System.out.print("Done!");

    }
}


