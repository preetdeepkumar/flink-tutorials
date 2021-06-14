package org.pd.streaming.graph;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.graph.library.SingleSourceShortestPaths;

import java.util.Random;

public class GraphApiExample
{
    private static final String user1Name = "Vipul";

    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String graph_path ="file:///C://graph_data//graph_data.txt";

        /* format: user, friend */
        DataSet<Tuple2<String, String>> friends = env.readTextFile(graph_path)
                .map(new MapFunction<String, Tuple2<String, String>>()
                {
                    public Tuple2<String, String> map(String value)
                    {
                        String[] words = value.split("\\s+");
                        return new Tuple2<String, String>(words[0], words[1]);
                    }
                });

        /* prepare normal dataset to edges for graph */
        DataSet<Edge<String, NullValue>> edges = friends.map(new MapFunction<Tuple2<String, String>, Edge<String, NullValue>>()
        {
            public Edge<String, NullValue> map(Tuple2<String, String> value)
            {
                Edge<String, NullValue> edge = new Edge();
                edge.setSource(value.f0); // user
                edge.setTarget(value.f1); // friend

                // return an edge between user and friend
                return edge;
            }
        });

        /* create graph from edges dataset */
        Graph<String, NullValue, NullValue> friendsGraphs = Graph.fromDataSet(edges, env);


        Graph<String, NullValue, Double> weightedfriendsGraph = friendsGraphs.mapEdges(new MapFunction<Edge<String, NullValue>, Double>()
        {

            public Double map(Edge<String, NullValue> edge) throws Exception
            {
                return 1.0;
            }
        });

        /* get all friend of friends of friends of....*/
        SingleSourceShortestPaths<String, NullValue> s1 = new SingleSourceShortestPaths<String, NullValue>(user1Name, 10);

        DataSet<Vertex<String, Double>> result = s1.run(weightedfriendsGraph);

        /* get only friends of friends for Vipul */
        DataSet<Vertex<String, Double>> fOfUser1 = result.filter(new FilterFunction<Vertex<String, Double>>()
        {
            public boolean filter(Vertex<String, Double> value)
            {
                if (value.f1 == 2)
                    return true;
                else
                    return false;
            }
        });

        Random random = new Random();
        String output_path = "file:///C://graph_data//graph_data-output-" + random.nextInt()+".txt";
        fOfUser1.writeAsText(output_path);

        env.executeAsync("Graph API Example");
        System.out.println("Done!, a file was created at " + output_path);
    }

}


