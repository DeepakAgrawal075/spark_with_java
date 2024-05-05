package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class ReduceByKey {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> lines = new JavaSparkContext(spark.sparkContext()).textFile("src/main/resources/data.txt");
        JavaPairRDD<String, Integer> languageCountPair = lines.mapToPair(l -> new Tuple2<>(l, 1));
        System.out.println(languageCountPair.collect());
        JavaPairRDD<String, Integer> reducedPair = languageCountPair.reduceByKey((a, b) -> a + b);
        System.out.println(reducedPair.collect());
    }
}
