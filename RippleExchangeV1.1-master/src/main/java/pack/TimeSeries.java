package pack;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

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

    public static JavaTimeSeriesRDD<String> creatSeries(DataFrame df2, JavaSparkContext context, SQLContext sqlContext) {
      /*  SparkConf conf = new SparkConf().setAppName("Spark-TS Ticker Example").setMaster("local");
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
*/
        //DataFrame tickerObs = loadObservations(context, sqlContext, "/home/eoin/Documents/Intellij Projects/spark-ts-examples-master/data/ticker.tsv");

        // Create an daily DateTimeIndex over August and September 2015 //test("accounts") !=null
        ZoneId zone = ZoneId.systemDefault();



        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String start1 = "2016-01-01T00:00:00Z";
        String end1 = "2016-02-01T00:00:00Z";
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        try {
            Date startDate = formatter.parse(start1);
            Date endDate = formatter.parse(end1);
            LocalDateTime start = startDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            LocalDateTime end = endDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();


            Timestamps stamps = new Timestamps();
            String jStr = "{" + '"' + "dateArray" + '"' + ":[";
            for (LocalDateTime date = start; date.isBefore(end); date = date.plusDays(1)) {
                // Do your job here with `date`.
//"\'{\"hello\":\"world\"}\'"
// "{\"hello\":\"world\"}"
                //"payments":1361.0
                // jStr+="{"+'"'+"payments"+'"'+":"+"null"+", ";
                jStr += "{" + '"' + "payments" + '"' + ":" + "0.0" + ", ";
                jStr += '"' + "key" + '"' + ":" + '"' + (Timestamp.valueOf(date)) + '"' + ", ";

                jStr += '"' + "date" + '"' + ":" + '"' + (Timestamp.valueOf(date)) + '"';
                if (!date.plusDays(1).isEqual(end))
                    jStr += "},\n";
                else {
                    jStr += "}";
                }

                //stamps.addDate(Timestamp.valueOf(date));

                //date.format(formatter1)
            }
            jStr += "]}";
            System.out.println(jStr);
            // jStr = jStr.replace("\\", "");
            JsonObject jDatesObj = (new JsonParser()).parse(jStr).getAsJsonObject();
            //  JsonObject stampsJson=gson.toJson(stamps);
            //System.out.println("stamps string"+stamps.toString())

            try (Writer writer = new FileWriter("Output.json")) {
                //BufferedWriter bw = new BufferedWriter(writer);
                // bw.flush();
                GsonBuilder gbuild = new GsonBuilder().serializeNulls();
                gbuild.setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                Gson gson = gbuild.create();

                gson.toJson(jDatesObj, writer);
                //bw.close();
            } catch (IOException e) {
                e.getCause();
            }


            // JavaRDD<String> forecastDates=context.parallelize(stamps.getTimeList());
            // DataFrame df = sqlContext.read().json()
            //df2.unionAll(forecastDates);
        } catch (ParseException p) {
            p.printStackTrace();
        }

        DataFrame addJson = sqlContext.read().json("Output.json");

        DataFrame formSimilar = addJson.select(org.apache.spark.sql.functions.explode(addJson.col("dateArray")));
        formSimilar.printSchema();
        formSimilar.show();
        DataFrame makeSimilar = formSimilar.select(
                formSimilar.col("col").getField("payments"), formSimilar.col("col").getField("key"), formSimilar.col("col").getField("date"));
        formSimilar.printSchema();
        formSimilar.show();

        // df2.drop("exchanges").drop("accounts").unionAll(makeSimilar);

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
                ZonedDateTime.of(LocalDateTime.parse("2017-03-17T00:00:00"), zone),
                new BusinessDayFrequency(1, 0));

        // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD


        df2.printSchema();

      /*  JsonBuilderFactory factory = Json.createBuilderFactory(config);
        JsonObject datesobject = Json.createObjectBuilder().build();
*/

        // DataFrame datesProjected = sqlContext.read().json(projectedDate);

        // df2.unionAll(datesProjected);
        DataFrame df3 = df2.drop("exchanges").drop("accounts").unionAll(makeSimilar).withColumnRenamed("payments", "oldpayments").withColumnRenamed("date", "olddate")
                .withColumnRenamed("key", "oldkey");

        DataFrame df4 = df3.withColumn("payments", df3.col("oldpayments").cast("double")).drop("oldpayments")
                .withColumn("key", df3.col("oldkey").cast("String")).drop("oldkey")
                .withColumn("date", df3.col("olddate").cast("timestamp")).drop("olddate");
        df4.printSchema();

        JavaTimeSeriesRDD tickerTsrdd = JavaTimeSeriesRDDFactory.timeSeriesRDDFromObservations(dtIndex, df4, "date", "key", "payments");

        // Cache it in memory
        tickerTsrdd.cache();
        System.out.println("ticker" + tickerTsrdd.count());

        // Count the number of series (number of symbols)
//        System.out.println(tickerTsrdd.count());

        // Impute missing values using linear interpolation

        JavaTimeSeriesRDD<String> filled = tickerTsrdd.fill("linear");
        System.out.println(filled.collect());

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