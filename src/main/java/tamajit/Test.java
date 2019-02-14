package tamajit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ibm on 5/3/2018.
 */
public class Test {
    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[3]");

//        conf.set("fs.local.block.size","300m");
//        conf.set("mapred.min.split.size","20");
//        System.out.println(conf.get("fs.local.block.size"));
//        conf.set("spark.default.parallelism","2");
        Tuple2<String, String>[] all = conf.getAll();
        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.hadoopConfiguration().set("dfs.block.size", "9437184");
        sc.hadoopConfiguration().set("fs.local.block.size", "9437184");
        System.out.println("dfs.block.size:"+sc.hadoopConfiguration().get("dfs.block.size"));
        System.out.println("fs.local.block.size:"+sc.hadoopConfiguration().get("fs.local.block.size"));
        for (int i = 0; i < all.length; i++) {
            System.out.println(all[i]);
        }
        JavaRDD<String> intRDD = sc.textFile("in/test");

        System.out.println("number of partitions:::::::::::::::::::::::" + intRDD.getNumPartitions());
        sc.close();
        System.out.println("done");
    }
}
