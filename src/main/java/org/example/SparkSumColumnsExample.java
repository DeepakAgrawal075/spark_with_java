package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SparkSumColumnsExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Spark Sum Columns Example")
                .master("local")
                .getOrCreate();

        // Sample DataFrame creation
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                new Record("A", "X", 10, 5),
                new Record("A", "X", 20, 15),
                new Record("A", "Y", 30, 25),
                new Record("B", "X", 40, 35),
                new Record("B", "Y", 50, 45)
        ), Record.class);

        // Define group-by columns and sum columns
        List<String> groupByColumns = Arrays.asList("key1", "key2");
        List<String> sumColumns = Arrays.asList("value1", "value2");

        // Perform the group-by and sum operation
        Dataset<Row> result = groupByAndSum(df, groupByColumns, sumColumns);

        // Show the result
        result.show();

        spark.stop();
    }

    public static Dataset<Row> groupByAndSum(Dataset<Row> df, List<String> groupByColumns, List<String> sumColumns) {
        // Create an array of Column objects for group-by
        org.apache.spark.sql.Column[] groupByCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(org.apache.spark.sql.Column[]::new);

        // Create an array of aggregation expressions for summing the specified columns
        org.apache.spark.sql.Column[] sumExprs = sumColumns.stream()
                .map(col -> functions.sum(functions.col(col)).alias(col))
                .toArray(org.apache.spark.sql.Column[]::new);

        // Group by the specified columns and aggregate by summing the specified columns
        Dataset<Row> aggregatedDf = df.groupBy(groupByCols)
                .agg(sumExprs[0], Arrays.copyOfRange(sumExprs, 1, sumExprs.length));

        return aggregatedDf;
    }

    // Sample record class
    public static class Record implements Serializable {
        private String key1;
        private String key2;
        private int value1;
        private int value2;

        public Record(String key1, String key2, int value1, int value2) {
            this.key1 = key1;
            this.key2 = key2;
            this.value1 = value1;
            this.value2 = value2;
        }

        // Getters and setters
        public String getKey1() { return key1; }
        public void setKey1(String key1) { this.key1 = key1; }

        public String getKey2() { return key2; }
        public void setKey2(String key2) { this.key2 = key2; }

        public int getValue1() { return value1; }
        public void setValue1(int value1) { this.value1 = value1; }

        public int getValue2() { return value2; }
        public void setValue2(int value2) { this.value2 = value2; }
    }
}
