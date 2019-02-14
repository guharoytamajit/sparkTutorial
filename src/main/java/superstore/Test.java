package superstore;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by ibm on 5/3/2018.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> superstore = sc.textFile("in/superstore.csv");



        JavaRDD<String> airportsNameAndCityNames = superstore.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[17]}, ",");
                }
        );


        airportsNameAndCityNames.saveAsTextFile("out/salesdata.text");

        sc.close();
        System.out.println("done");
    }
}
