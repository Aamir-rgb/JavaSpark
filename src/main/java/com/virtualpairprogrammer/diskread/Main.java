package com.virtualpairprogrammer.diskread;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .collect().forEach(System.out::println);

        System.out.println("Unique Word to Count");
        JavaRDD<String> nextRDD = sc.textFile("src/main/resources/subtitles/boringwords.txt");
        long counterValue = nextRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .collect().stream().distinct().count();
        System.out.println(counterValue);

        JavaRDD<String> lettersonlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]","").toLowerCase());

        JavaRDD<String> removedBlankLines = lettersonlyRDD.filter(sentences -> sentences.trim().length() > 0);
        removedBlankLines.collect().forEach(System.out::println);

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        List<String> results = justWords.take(50);
        results.forEach(System.out::println);

        JavaRDD<String> justInterestingWords = justWords.filter(word -> Util.isNotBoring(word));
        JavaPairRDD<String,Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<String,Long>(word,1L));

        JavaPairRDD<String,Long> totals = pairRDD.reduceByKey((value1,value2) -> value1+value2);

        JavaPairRDD<Long,String> switched = totals.mapToPair(tuple -> new Tuple2<Long,String>(tuple._2,tuple._1));


        JavaPairRDD<Long,String> sorted = switched.sortByKey(false);

        sorted = sorted.coalesce(1);
        List<Tuple2<Long,String>> result = sorted.take(50);
        result.forEach(System.out::println);
    }
}
