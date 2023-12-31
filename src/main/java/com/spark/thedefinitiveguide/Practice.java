package com.spark.thedefinitiveguide;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;


public class Practice {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark The Definitive Guide").master("local[*]")
                .getOrCreate();
        Dataset<Row> staticDataset = spark.read().option("header","true").option("inferSchema","true").csv("src/main/resources/retail-data/by-day/*.csv");
      staticDataset.createOrReplaceTempView("retail_data");
        StructType staticSchema = staticDataset.schema();


        staticDataset.selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate")
                .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
                .sum("total_cost")
                .show(5);

        spark.conf().set("spark.sql.shuffle.partitions","5");

      Dataset<Row> streamingDataFrame = spark.readStream().schema(staticSchema)
              .option("maxFilesPerTrigger","1")
              .format("csv")
              .option("header","true")
              .load("src/main/resources/retail-data/by-day/*.csv");
      System.out.println(streamingDataFrame.isStreaming());

      spark.stop();

    }
}
