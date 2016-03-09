package pack;
//Import Spark SQL
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
//import the JavaSchemaRDD
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
public class Json2SparkSQL {
    SparkConf sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]");
   // SparkConf conf = new SparkConf().setAppName("Simple Application");
      //      conf.setMaster("spark://myhost:7077");

    HiveContext hiveCtx = new HiveContext(SparkContext.getOrCreate(sparkConf));


    public void dataFrame() {
    DataFrame input = hiveCtx.jsonFile("/home/eoin/Documents/JsonFiles/file1.txt");
        input.printSchema();

    }


}
