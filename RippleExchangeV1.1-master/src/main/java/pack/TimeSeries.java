//This can be achieved via the use of JSP custom tags in conjunction with tag handlers and w
package pack;

import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;
import com.cloudera.sparkts.models.ARIMA;
import com.cloudera.sparkts.models.ARIMAModel;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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
import java.util.Date;


public class TimeSeries {


    public static JavaTimeSeriesRDD
    creatSeries(DataFrame df2, JavaSparkContext context, SQLContext sqlContext,
                String today, String end1, String start1, JavaRDD lines) {
//Code below can be used to create a timeseries rdd
        //was not used in the ARIMA forecasting model
        //but potentially useful fo further development
//start of essentially redundant code***********************************************************************************
        ZoneId zone = ZoneId.systemDefault();


        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        try {
            Date startDate = formatter.parse(today);
            Date endDate = formatter.parse(end1);
            LocalDateTime start = startDate.toInstant()
                    .atZone(ZoneId.systemDefault()).toLocalDateTime();
            LocalDateTime end = endDate.toInstant().atZone(ZoneId.systemDefault())
                    .toLocalDateTime();

            //create a json array with the timestamps and 0.0 for the payments
            //to be filled in
            String jStr = "{" + '"' + "dateArray" + '"' + ":[";
            for (LocalDateTime date = start; date.isBefore(end); date = date.plusDays(1)) {

                //jStr += "{" + '"' + "payments" + '"' + ":" + "null" + ", ";
                jStr += "{" + '"' + "payments" + '"' + ":" + "0.0" + ", ";
                jStr += '"' + "key" + '"' + ":" + '"' + (Timestamp.valueOf(date)) + '"' + ", ";

                jStr += '"' + "date" + '"' + ":" + '"' + (Timestamp.valueOf(date)) + '"';
                if (!date.plusDays(1).isEqual(end))
                    jStr += "},\n";
                else {
                    jStr += "}";
                }

            }
            jStr += "]}";
            System.out.println(jStr);
            //parse it to an object
            JsonObject jDatesObj = (new JsonParser()).parse(jStr).getAsJsonObject();


            try (Writer writer = new FileWriter("Output.json")) {

                GsonBuilder gbuild = new GsonBuilder().serializeNulls();
                gbuild.setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                Gson gson = gbuild.create();

                gson.toJson(jDatesObj, writer);

            } catch (IOException e) {
                e.getCause();
            }

        } catch (ParseException p) {
            p.printStackTrace();
        }

        DataFrame addJson = sqlContext.read().json("Output.json");

        DataFrame formSimilar = addJson.select(org.apache.spark.sql.functions
                .explode(addJson.col("dateArray")));
        formSimilar.printSchema();
        formSimilar.show();
        DataFrame makeSimilar = formSimilar.select(
                formSimilar.col("col").getField("payments").cast("double"),
                formSimilar.col("col").getField("key"),
                formSimilar.col("col").getField("date"));
        formSimilar.printSchema();
        formSimilar.show();


        DateTimeIndex dtIndex = DateTimeIndexFactory.uniformFromInterval(
                ZonedDateTime.of(LocalDateTime.parse(start1 + "T00:00:00"), zone),
                ZonedDateTime.of(LocalDateTime.parse(end1 + "T00:00:00"), zone),
                new BusinessDayFrequency(1, 1));


        DataFrame df3 = df2.drop("exchanges").drop("accounts").unionAll(makeSimilar)
                .withColumnRenamed("payments", "oldpayments")
                .withColumnRenamed("date", "olddate")
                .withColumnRenamed("key", "oldkey");

        DataFrame df4 = df3.withColumn("payments", df3.col("oldpayments").cast("double"))
                .drop("oldpayments")
                .withColumn("key", df3.col("oldkey").cast("String")).drop("oldkey")
                .withColumn("date", df3.col("olddate").cast("timestamp")).drop("olddate");
        df4.printSchema();
// Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
        JavaTimeSeriesRDD tickerTsrdd = JavaTimeSeriesRDDFactory
                .timeSeriesRDDFromObservations(dtIndex, df4, "date", "key", "payments");

        // Cache it in memory
        tickerTsrdd.cache();
        System.out.println("ticker" + tickerTsrdd.count());
        JavaTimeSeriesRDD filled = tickerTsrdd.fill("linear");
        filled.returnRates();

//end of essentially redundant code***********************************************************************************

        // The dataset is sampled from an ARIMA(p, d, q) model

        System.out.println("\n******************************************");
        System.out.println(lines.count());
        System.out.println(lines.collect());
        System.out.println("\n******************************************");

        JavaDoubleRDD vals = lines.mapToDouble(line -> {
                    try {

                        String[] tokens = line.toString().replace("[", "").replace("]", "").split(",");

                        return Double.parseDouble(tokens[0]);

                    } catch (Exception ex) {
                        return Double.parseDouble("0.0");

                    }
                }
        );


        System.out.println("\n******************************************");
        System.out.println("Count is:" + vals.count());
        System.out.println(vals.collect());
        System.out.println("\n******************************************");


        Vector ts = Vectors.dense(vals.toArray().stream().mapToDouble(d -> d).toArray());
        ARIMAModel arimaModel = ARIMA.autoFit(ts, 10, 10, 10);

        System.out.println("coefficients: " + java.util.Arrays.toString(arimaModel.coefficients()));
        Vector forecast = arimaModel.forecast(ts, 20);
        System.out.print("forecast of next observations: "
                + java.util.Arrays.toString(forecast.toArray()));


        SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date startDate = formatter1.parse(today);

            LocalDateTime start = startDate.toInstant()
                    .atZone(ZoneId.systemDefault()).toLocalDateTime();

            //create a json array with the timestamps and the payments
            //to be filled in
            String jStr1 = "[";
            //for (LocalDateTime date = start; date.isBefore(end); date = date.plusDays(1)) {
            JavaRDD json = lines.map(line -> {
                        String[] jsonArr = line.toString().replace("[", "{").replace("]", "}").split(",");

                        return '"' + "payments:" + '"' + jsonArr[0] + ',' + '"' + "key:" + '"' + jsonArr[1]
                                + ',' + '"' + "date:" + '"' + jsonArr[2] + ',';

                    }
            );
            jStr1 += json.toString();
            int i = 0;
            for (double v : forecast.toArray()) {
                i++;
                jStr1 += "{" + '"' + "payments" + '"' + ":" + v + ", ";

                jStr1 += '"' + "date" + '"' + ":" + '"' + (Timestamp.valueOf(start)).toString().substring(0, 9) + '"';
                if (i < forecast.toArray().length)
                    jStr1 += "},\n";
                else {
                    jStr1 += "]";
                }
                start.plusDays(1);
            }
            jStr1 += "]}";
            System.out.println("JSON******!" + jStr1);

            try (Writer writer = new FileWriter
                    ("/home/eoin/Documents/Intellij Projects/RippleExchangeV1.1-master/web/WEB-INF/output.txt")) {
                try (JsonWriter jWrite = new JsonWriter(writer)) {
                    //parse it to an object
                    JsonObject jDatesObj = (new JsonParser()).parse(jStr1).getAsJsonObject();
                    GsonBuilder gbuild = new GsonBuilder();
                    gbuild.setDateFormat("yyyy-MM-dd");
                    Gson gson1 = gbuild.create();

                    gson1.toJson(jDatesObj, jWrite);


                } catch (IOException e) {
                    e.getCause();
                }
            } catch (IOException r) {
                r.getCause();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }


        return filled;

    }
}