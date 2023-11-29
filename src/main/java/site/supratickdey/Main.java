package site.supratickdey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(53);
        inputData.add(954);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("ASpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myRdd.map((value) -> Math.sqrt(value));

        sqrtRdd.foreach((value) -> System.out.println(value));

        System.out.println(result);

        sc.close();
    }
}