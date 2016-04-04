package pack;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;


public class TimeSeries {
   /* private static DataFrame loadObservations(JavaSparkContext sparkContext, SQLContext sqlContext,
                                              String path) {
        JavaRDD<Row> rowRdd = sparkContext.textFile(path).map((String line) -> {
            String[] tokens = line.split("\t");
            ZonedDateTime dt = ZonedDateTime.of(Integer.parseInt(tokens[0]),
                    Integer.parseInt(tokens[1]), Integer.parseInt(tokens[1]), 0, 0, 0, 0,
                    ZoneId.systemDefault());
            String symbol = tokens[3];
            double price = Double.parseDouble(tokens[5]);
            return RowFactory.create(Timestamp.from(dt.toInstant()), symbol, price);
        });
        List<StructField> fields = new ArrayList();
        fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("symbol", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("price", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);
        return sqlContext.createDataFrame(rowRdd, schema);
    }*/

    public static JavaTimeSeriesRDD<String> creatSeries(DataFrame df2, JavaSparkContext context) {
      /*  SparkConf conf = new SparkConf().setAppName("Spark-TS Ticker Example").setMaster("local");
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
*/
        //DataFrame tickerObs = loadObservations(context, sqlContext, "/home/eoin/Documents/Intellij Projects/spark-ts-examples-master/data/ticker.tsv");

        // Create an daily DateTimeIndex over August and September 2015 //test("accounts") !=null
        ZoneId zone = ZoneId.systemDefault();

        //
      /*  List<String> testDates = test.toJavaRDD().map(
                new Function<Row, String>() {
                    public String call(Row row) {
                        return row.getString(0);
                    }
                }).collect();*/
      /*  ListIterator listite = testDates.listIterator();
        for(listite: s ) {
            DateTimeIndex dti = DateTimeIndexFactory.fromString();
        }*/


        DateTimeIndex dtIndex = DateTimeIndexFactory.uniformFromInterval(
                ZonedDateTime.of(LocalDateTime.parse("2014-08-31T00:00:00"), zone),
                ZonedDateTime.of(LocalDateTime.parse("2016-03-17T00:00:00"), zone),
                new BusinessDayFrequency(24, 0));

        // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD

        JavaTimeSeriesRDD tickerTsrdd = JavaTimeSeriesRDDFactory.javaTimeSeriesRDDFromObservations(
                dtIndex, df2, "payments", "accounts", "exchanges");

        // Cache it in memory
        tickerTsrdd.cache();

        // Count the number of series (number of symbols)
//        System.out.println(tickerTsrdd.count());

        // Impute missing values using linear interpolation
        JavaTimeSeriesRDD<String> filled = tickerTsrdd.fill("linear");

        return filled;
/*filled.map(new Function<Row, String>() {
    @Override
    public String call(Row row) throws Exception {
        // todo: select whichever fields from the sessionsAgg that should be included in the vectors
        //long a = row.getLong(row.fieldIndex("accounts"));
        String e = row.getString(row.fieldIndex("exchanges"));
//                long l = row.getLong(row.fieldIndex("ledger"));
        String p = row.getString(row.fieldIndex("payments"));
        String d = row.getString(row.fieldIndex("date"));
        String result=e+"\"
        return result;
    }
});
        List<String> oneItemList = new LinkedList<>();
        oneItemList.add(filled.toRowMatrix().toString());
        JavaRDD<String> result = context.parallelize(oneItemList);
        result.saveAsTextFile("/home/eoin/Documents/test.csv");
        return filled;
        // Compute return rates
       // JavaTimeSeriesRDD<String> returnRates = filled.returnRates();
    }*/
    }
}