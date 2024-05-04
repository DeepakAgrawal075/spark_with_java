package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> datasetOfRowObjects = spark
                .read()
                .format("json")
                .load("/Applications/MAMP/htdocs/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json");
        List<Row> rows = datasetOfRowObjects.collectAsList();
        System.out.println(rows.size());
    }
}