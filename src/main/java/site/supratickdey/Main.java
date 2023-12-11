package site.supratickdey;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("ASpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd =  sc.textFile("src/main/resources/subtitles/input.txt");



        /*
        Verbose implementation
        JavaRDD<String> sentences = sc.parallelize(inputData);

        JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);

        filteredWords.foreach(value-> System.out.println(value));
        */

        initialRdd
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);


        sc.close();
    }
}