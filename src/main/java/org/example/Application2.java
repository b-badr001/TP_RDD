package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class Application2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("tp total ventes").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        JavaPairRDD<String, Double> salesByCity = rddLines.mapToPair(rddLine -> {
            String[] parts = rddLine.split(" ");
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        }).reduceByKey(Double::sum);
        double sum = salesByCity.mapToDouble(tuple -> tuple._2).sum();
        System.out.println("Somme des ventes : " + sum);

    }
}
