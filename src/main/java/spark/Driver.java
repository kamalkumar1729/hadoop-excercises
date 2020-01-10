package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Driver {

    private static JavaSparkContext javaSparkContext=null;

    public static void main(String[] args) {
        javaSparkContext = new JavaSparkContext(new SparkConf().setAppName("myApp").setMaster("local"));
        System.out.println(javaSparkContext);
    }
}
