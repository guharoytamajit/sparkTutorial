package superstore;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HousePriceSolution {

    private static final String PRODUCT_NAME = "Product Name";
    private static final String SALES = "Sales";
    private static final String CATEGORY = "Category";

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("SalesByCategory").master("local[*]").getOrCreate();

        Dataset<Row> superstore = session.read().option("header", "true").csv("in/superstore.csv");

        Dataset<Row> product_sales = superstore.select(col(CATEGORY), col(SALES).cast("decimal(10,2)"));

        product_sales.groupBy(CATEGORY)
                        .agg(sum(SALES))
                        .show();
    }
}
