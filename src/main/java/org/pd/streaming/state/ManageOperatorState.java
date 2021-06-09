package org.pd.streaming.state;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ManageOperatorState implements SinkFunction < Tuple2 < String, Integer >> , CheckpointedFunction {
    private final int threshold;
    private transient ListState < Tuple2 < String, Integer >> checkpointedState;

    private final List < Tuple2 < String, Integer >> bufferedElements;

    public ManageOperatorState(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList < Tuple2 < String, Integer >> ();
    }

    public void invoke(Tuple2 < String, Integer > value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Tuple2 < String, Integer > element: bufferedElements) {
                // send it to the sink

            }
            bufferedElements.clear();
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2 < String, Integer > element: bufferedElements) {
            checkpointedState.add(element);
        }
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor < Tuple2 < String, Integer >> descriptor = new ListStateDescriptor < Tuple2 < String, Integer >> ("buffered-elements",
                TypeInformation.of(new TypeHint < Tuple2 < String, Integer >> () {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor); // .getUninonListState(descriptor)

        if (context.isRestored()) {
            for (Tuple2 < String, Integer > element: checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
