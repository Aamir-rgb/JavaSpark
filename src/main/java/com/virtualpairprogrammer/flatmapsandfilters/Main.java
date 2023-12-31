package com.virtualpairprogrammer.flatmapsandfilters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputDataString = new ArrayList<>();
        inputDataString.add("WARN: Tuesday 4 September 0405");
        inputDataString.add("ERROR: Tuesday 4 September 0408");
        inputDataString.add("FATAL: Wednesday 5 September 1632");
        inputDataString.add("ERROR: Friday 7 September 1854");
        inputDataString.add("WARN: Saturday 8 September 1942");

        JavaRDD<String> sentences = sc.parallelize(inputDataString);

        JavaRDD<String> words   = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());


        words.collect().forEach(System.out::println);

        //Filter Section
         System.out.println("Filter Section");
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
        filteredWords.collect().forEach(System.out::println);
    }
}
