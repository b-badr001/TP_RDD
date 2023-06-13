package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Application3 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("tp total ventes").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        String annee = "2023";
        JavaRDD<String> filteredLines = rddLines.filter(rddLine -> rddLine.startsWith(annee));

        JavaRDD<String> salesByCity = filteredLines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1];
            double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        }).reduceByKey(Double::sum).map(tuple -> tuple._1 + ": " + tuple._2);

        salesByCity.foreach(System.out::println);
        sc.close();
    }
}
