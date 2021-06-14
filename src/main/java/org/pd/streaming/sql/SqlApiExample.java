package org.pd.streaming.sql;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

import java.util.Random;

public class SqlApiExample {

    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        String avg_path ="file:///C://table_data//avg.txt";

        /* create table from csv */
        TableSource tableSrc = CsvTableSource.builder()
                .path(avg_path)
                .fieldDelimiter(",")
                .field("date", Types.STRING)
                .field("month", Types.STRING)
                .field("category", Types.STRING)
                .field("product", Types.STRING)
                .field("profit", Types.INT)
                .build();

        tableEnv.registerTableSource("CatalogTable", tableSrc);

        String sql = "SELECT `month`, SUM(profit) AS sum1 FROM CatalogTable WHERE category = 'Category5'"
                + " GROUP BY `month` ORDER BY sum1";
        Table order20 = tableEnv.sqlQuery(sql);

        DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);
        Random random = new Random();
        String output_path = "file:///C://table_data//table-sql" + random.nextInt()+".txt";
        order20Set.writeAsText(output_path);
        env.executeAsync("SQL API Example");
        System.out.println("Done!, a file was written at " + output_path);

    }

    public static class Row1
    {
        public String month;
        public Integer sum1;

        public Row1(){}

        public String toString()
        {
            return month + "," + sum1;
        }
    }

}



