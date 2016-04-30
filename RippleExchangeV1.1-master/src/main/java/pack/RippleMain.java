
package pack;

import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.*;

//The driver class
public class RippleMain {
    //date variables passed through the tagHadler from the JSP
    private static String sdate;
    private static String edate;

    public String getStart() {
        return sdate;
    }

    //setters and getters
    public static void setStart(String sdate1) {
        sdate = sdate1;
    }

    public String getEnd() {
        return edate;
    }

    public static void setEnd(String edate1) {
        edate = edate1;
    }

    //This method get todays date minus one day
    public String getToday() {
        DateTimeFormatter dateFormat =
                DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        DateTime today = new DateTime(DateTimeZone.UTC);
        today.minusDays(1);
        DateTime today1 = dateFormat.parseDateTime(today.toString().substring(0, 19) + 'Z');

        System.out.println(today1.toString().substring(0, 19));
        return today1.toString().substring(0, 19);
    }


    public void main(String[] args) throws IOException {
        //hardcoded variables
        //could incorporate them as options to be selected on JSP easily
        String base = "XRP";
        //selects currency and default gateway for the transaction data
        String counter = "EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
        //to default to current date, remove date variable
        String date = "2015-01-13T19:57:00Z";

        //select whether we are looking for exchange rates or stats
        String apiMethod = "stats";

        //an array list to hold the url API queries we wish to make
        ArrayList<URL> URLList = new ArrayList<URL>();

        //select which api method you want to retrieve
        //exchange_rates or stats
        //this allows for extensibility
        URL rippleUrl = null;
        ApiMethod objType = null;
        switch (apiMethod) {
            //create the link to the URL
            case ("exchange_rates"):
                rippleUrl = new URL("https://data.ripple.com/v2/" +
                        apiMethod + "/" + base + "/" + counter + "?" + date);
                System.out.println("Exchange rates");

                break;
            case ("stats"):
                //variables used to test without servlet running
                String start = "2016-03-15";
                String end = "2014-08-31";
                //hardcoded data
                String interval = "hour";
                System.out.println("Statistics");

                URLList = APIQueries.createQueries(apiMethod,
                        sdate, this.getToday().substring(0, 10), interval);

                break;

            default:
                System.out.println("no url specified");

        }
        //Initialise a connection to the mySQL database
        Connection myConn = null;

        //Get a connection to the MySQL Database using the jdbc Driver
        try {
            Class.forName("com.mysql.jdbc.Driver");

            myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo?autoReconnect=true&useSSL=false", "root", "Scorpio21*");
        } catch (Exception e) {
            e.printStackTrace();
        }
        //wipe data written to the database each time before the program is run
        try {
            PreparedStatement preparedStatement =
                    myConn.prepareStatement("delete from test where exchanges is not null");

            preparedStatement.executeUpdate();

        } catch (SQLException se) {
            se.printStackTrace();
        }
        //initialse varaibles
        Statement statement = null;
        URL rippleURL = null;
        //iterate through URLS creating JSONS and storing them to a MySQL table
        Iterator iter = URLList.iterator();
        while (iter.hasNext()) {
            rippleUrl = (URL) iter.next();

            //parsing the json from the url
            JsonObject jsonObject = RippleExchange
                    .jsonParse(RippleExchange.urlJsonString((rippleUrl)));
            //using sql to insert data
            try {
                //Statement to pass to MySQL terminal
                PreparedStatement preparedStatement = myConn
                        .prepareStatement("INSERT INTO test (exchanges) VALUES(?)");
                //insert the json as a string
                preparedStatement.setString(1, jsonObject.toString());

                preparedStatement.executeUpdate();

            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        //reading from mysql to spark dataframe
        //put the data into a HashMap as an intermediate
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://localhost:3306/demo?autoReconnect=true&useSSL=false&user=root&password=Scorpio21*");
        options.put("dbtable", "test");
        //Create a spark context
        JavaSparkContext sc = new JavaSparkContext
                (new SparkConf().setAppName("DBConnection").setMaster("local[*]"));
        //Create an SQL context
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame jdbcDF = sqlContext.jdbc(options.get("url"), options.get("dbtable"));

        //collect the data from the dataframe as a matrix of rows
        //each representing a json
        Row[] rows = jdbcDF.collect();
        List<String> data = new LinkedList<>();
        for (Row row2 : rows) {
            data.add(row2.toString());
        }
        //parallelize this data so we can use parallel programing
        //The elements of the collection are copied to form a
        // distributed dataset that can be operated on in parallel
        JavaRDD<String> distData = sc.parallelize(data);
        //we can use sqlcontext to access spark data
        DataFrame df = sqlContext.read().json(distData);
        //manipulate the data to access what is relevant to us
        DataFrame personPositions = df.select(df.col("stats").as("stat"),
                org.apache.spark.sql.functions.explode(df.col("stats")).as("stat1"));

        DataFrame test = personPositions.select(
                personPositions.col("stat1").getField("accounts_created").as("accounts"),
                personPositions.col("stat1").getField("exchanges_count").as("exchanges"),
                personPositions.col("stat1").getField("ledger_count").as("ledger"),
                personPositions.col("stat1").getField("payments_count").as("payments"),
                personPositions.col("stat1").getField("date").as("date"));


        test.drop("ledger");

        //casting
        DataFrame df1 = test.withColumnRenamed("accounts", "oldaccounts")
                .withColumnRenamed("exchanges", "oldexchanges")
                .withColumnRenamed("payments", "oldpayments")
                .withColumnRenamed("date", "olddate").drop("ledger");
        DataFrame df2 = df1.drop("oldaccounts").drop("oldexchanges")
                .withColumn("payments", df1.col("oldpayments").cast("double"))
                .drop("oldpayments")
                .withColumn("key", df1.col("olddate").cast("String"))
                .withColumn("date", df1.col("olddate").cast("timestamp")).drop("olddate");
        System.out.println("df2 " + df2.count());
        //save the data as a text file for later use
        df2.toJSON().saveAsTextFile("/home/eoin/Documents/Intellij Projects/df2.json");
        System.out.println("*************************8df2schme");
        df2.printSchema();
        JavaRDD lines = df2.toJavaRDD();
        System.out.println(lines.take(24));
        //TimeSeries.creatSeries returns a JavaTimeSeriesRDD (from the SparkTS library)
        DataFrame cast1 = TimeSeries.creatSeries
                (df2, sc, sqlContext, this.getToday().substring(0, 10), this.getEnd(), this.getStart(), lines)
                .toObservationsDataFrame(sqlContext, "date", "key", "payments");


        cast1.printSchema();


        try {
            File source = new File("/home/eoin/Documents/Intellij Projects/data.json");
            File dest = new File("/home/eoin/Documents/Intellij Projects/RippleExchangeV1.1-master/web/WEB-INF/data.json");
            try {
                FileUtils.copyDirectory(source, dest);
            } catch (IOException e) {
                e.printStackTrace();
            }

          /*  System.out.println(cast1.toJavaRDD().count());

            DataFrame jsonArray = sqlContext.read()
                    .json("/home/eoin/Documents/Intellij Projects/test.txt");

            cast1.coalesce(1).toJSON().saveAsTextFile
                    ("/home/eoin/Documents/Intellij Projects/sample.json");*/
        } catch (ClassCastException e) {
            e.printStackTrace();
        }


    }


}

