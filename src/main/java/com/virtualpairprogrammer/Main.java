package com.virtualpairprogrammer;

import org.apache.log4j.Level;

import java.util.List;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRDD = sc.parallelize(inputData);

        Double result  = myRDD.reduce((value1,value2) -> value1+value2);

        System.out.println(result);

        //Mapping Operations

        List<Integer> inputDataInt = new ArrayList<>();
        inputDataInt.add(35);
        inputDataInt.add(12);
        inputDataInt.add(90);
        inputDataInt.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);


        JavaRDD<Integer> myRDDInt = sc.parallelize(inputDataInt);

        Integer resultInt  = myRDDInt.reduce((value1,value2) -> value1+value2);

        JavaRDD<Double> sqrtRDD = myRDDInt.map(value -> Math.sqrt(value));

       sqrtRDD.collect().forEach(System.out::println);

        System.out.println(sqrtRDD.count());

        //Count the elements in sqrtRDD
        JavaRDD<Long> singleIntegerRDD = sqrtRDD.map(value -> 1L);
        Long count = singleIntegerRDD.reduce((value1,value2) -> value1+value2);
        System.out.println(count);

        sc.close();
    }
}
