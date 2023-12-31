package com.virtualpairprogrammer;

import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RDDObjects {
    public static void main(String[] args) {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputDataInt = new ArrayList<>();
        inputDataInt.add(35);
        inputDataInt.add(12);
        inputDataInt.add(90);
        inputDataInt.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        JavaRDD<Integer> originalIntegers = sc.parallelize(inputDataInt);
        JavaRDD<Double> sqrtRDD = originalIntegers.map(value -> Math.sqrt(value));

        IntegerWithSquareRoot iws = new IntegerWithSquareRoot(9);
        JavaRDD<IntegerWithSquareRoot> sqrtRDD1 = originalIntegers.map(value -> new IntegerWithSquareRoot(value));
        JavaRDD<Tuple2<Integer,Double>> sqrtRDD2 = originalIntegers.map(value -> new Tuple2<>(value,Math.sqrt(value)));

        List<String> inputDataString = new ArrayList<>();
        inputDataString.add("WARN: Tuesday 4 September 0405");
        inputDataString.add("ERROR: Tuesday 4 September 0408");
        inputDataString.add("FATAL: Wednesday 5 September 1632");
        inputDataString.add("ERROR: Friday 7 September 1854");
        inputDataString.add("WARN: Saturday 8 September 1942");

//        JavaRDD<String> originalLogMessages = sc.parallelize(inputDataString);
//        JavaPairRDD<String,Long> pairRDD = originalLogMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(":");
//            String level = columns[0];
//            return new Tuple2<String,Long>(level,1L);
//        });
//
//        JavaPairRDD<String,Long>  sumsRDD = pairRDD.reduceByKey((value1,value2) -> value1+value2);
//
//        sumsRDD.foreach(tuple -> System.out.println(tuple._1+ " has "+ tuple._2));
//        sc.parallelize(inputDataString).mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0],1L))
//                .reduceByKey((value1,value2) -> value1+value2).foreach(tuple -> System.out.println(tuple._1+ " has "+ tuple._2));

        //GroupByKey Version
        sc.parallelize(inputDataString).mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0],1L))
                        .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1+ " has "+ Iterables.size(tuple._2) + " instances "));


        sc.close();
    }
}

