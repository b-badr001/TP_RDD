package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
    }
}