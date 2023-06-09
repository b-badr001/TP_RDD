package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application1 {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("TP 1 RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("badr","hamza","ayoub","salah","mehdi","sara","imane","ayman"));
        JavaRDD<String> rdd2 = rdd1.flatMap(word -> Arrays.asList(word.split(" ")).iterator());

        for (String word : rdd2.collect()) {
            System.out.println(word);
        }

        JavaRDD<String> rdd3 = rdd2.filter(word -> word.startsWith("a"));

        for (String word : rdd3.collect()) {
            System.out.println(word);
        }

        JavaRDD<String> rdd4 = rdd2.filter(word -> word.endsWith("a"));

        for (String word : rdd4.collect()){
            System.out.println(word);
        }

        JavaRDD<String> rdd5 = rdd3.union(rdd4);

        for (String word : rdd5.collect()){
            System.out.println(word);
        }

        JavaRDD<String> rdd6 = rdd5.map(word -> word.toUpperCase());

        for (String word : rdd6.collect()) {
            System.out.println(word);
        }

        JavaPairRDD<String, Integer> pairRDD = rdd6.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> sortedRDD = reducedRDD.sortByKey();

        for (Tuple2<String, Integer> pair : sortedRDD.collect()) {
            System.out.println(pair._1() + ": " + pair._2());
        }

        sc.stop();
    }
}