package org.example;

import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateRow {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("first_name", DataTypes.StringType, false),
                        DataTypes.createStructField("last_name", DataTypes.StringType, false)
                }
        );
        Row r1 = RowFactory.create("Alice", "Henderson");
        Row r2 = RowFactory.create("Bob", "Sanders");
        ImmutableList<Row> data = ImmutableList.of(r1, r2);
        Dataset<Row> rowDataset = spark.createDataFrame(data, schema);
        rowDataset.show();
        JavaRDD<Row> distributedData = new JavaSparkContext(spark.sparkContext()).parallelize(data);
        System.out.println(distributedData.count());
    }
}
