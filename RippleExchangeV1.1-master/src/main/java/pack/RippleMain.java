package pack;

import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;



public class RippleMain {
    public JsonObject jsonObject;

    public static void main(String[] args) throws IOException /*try*/ {

        String base = "XRP";
        String counter = "EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
        //to default to current date, remove date variable
        String date = "2015-01-13T19:57:00Z";
        //String date="2015-01-13";
        //   Date date = new Date();
        String apiMethod = "stats";
        String thisMoment = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        System.out.println(thisMoment);
        ArrayList<URL> URLList = new ArrayList<URL>();
        //protected String apiMethod = "exchange_rates";

        //select which api method you want to retrieve
        URL rippleUrl = null;
        ApiMethod objType = null;
        switch (apiMethod) {
            //create the link to the URL
            case ("exchange_rates"):
                rippleUrl = new URL("https://data.ripple.com/v2/" + apiMethod + "/" + base + "/" + counter + "?" + date);
                System.out.println("1");
                /*String str1=thisMoment.substring(0,19)+"Z";
                //LocalDateTime date1 = LocalDateTime.parse(str1, DateTimeFormatter.ISO_INSTANT.withLocale(Locale.ENGLISH));
                for(int i=0;i<24;i+=2){
                    //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ISO_INSTANT);
                    String str3="";
                    if(i<10){
                        str3="0";
                    }
                    String str2= str1.substring(0,11)+str3+i+str1.substring(13,19);
                   // date1.minusHours(2);
                   // date1.minusMinutes(1);
                    System.out.println(str2); // 2010-01-02
                }*/
                objType = new ExchangeRates();
                break;
            case ("stats"):
                //CAN GET LAST 200 RESUlTS
                String start = "2016-03-15";
                String end = "2014-08-31";
                String interval = "hour";
                // rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=2016-03-15&end=2014-08-31&interval=day&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());
               /* for(int i=0;i<20;i++)
                {

                    String modifiedDate= new SimpleDateFormat("yyyy-MM-dd").format(Instant.now().minusSeconds(31536000));
                    //!!!MOTHERFUCKER I JUST REALISE THAT THE QUERY IS ALREADY A SEARCH BETWEEN TWO DATES
                    rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=2016-03-30&end=2015-08-31&interval=hour&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());
                    URLList.add(rippleUrl);
                }*/
                System.out.println("2");
                URLList = APIQueries.createQueries(apiMethod, start, end, interval);
                objType = new Stats();
                break;

            default:
                System.out.println("no url");

        }
        //Create a connection to the database

        Connection myConn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo?autoReconnect=true&useSSL=false", "root", "Scorpio21*");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Statement statement = null;
        URL rippleURL = null;
        //iterate through URLS creating JSONS and storing them to a mySQL table
        Iterator iter = URLList.iterator();
        while (iter.hasNext()) {
            rippleUrl = (URL) iter.next();

            //parsing the json from the url
            JsonObject jsonObject = RippleExchange.jsonParse(RippleExchange.urlJsonString((rippleUrl)));
//Writing a json to a txt file
        /*FileWriter file = new FileWriter("/home/eoin/Documents/JsonFiles/file1.txt");
        try{
            file.write(String.valueOf(jsonObject));
            System.out.println("Sucessful copy");
            System.out.println("\nJSOn : "+jsonObject);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            file.flush();
            file.close();
        }*/
            try {
                PreparedStatement preparedStatement = myConn.prepareStatement("INSERT INTO test (exchanges) VALUES(?)");

                preparedStatement.setString(1, jsonObject.toString());

                int insertCount = 0;
                insertCount = preparedStatement.executeUpdate();

/*
            String st="{\"employees\":[\n" +
        "    {\"firstName\":\"John\", \"lastName\":\"Doe\"},\n" +
        "    {\"firstName\":\"Anna\", \"lastName\":\"Smith\"},\n" +
        "    {\"firstName\":\"Peter\", \"lastName\":\"Jones\"}\n" +
        "]}";
            String sql= "INSERT INTO test(data) VALUES("+st+")";

            statement.executeUpdate(sql);
*/
                //System.out.println("Database created");
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        //reading from mysql to spark dataframe
        //"jdbc:mysql://localhost:3306/demo?autoReconnect=true&useSSL=false", "root", "Scorpio21*"
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://localhost:3306/demo?autoReconnect=true&useSSL=false&user=root&password=Scorpio21*");
        options.put("dbtable", "test");
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("DBConnection").setMaster("local[*]"));
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        // DataFrame jdbcDF = sqlContext.load("jdbc", options).cache();
        DataFrame jdbcDF = sqlContext.jdbc(options.get("url"), options.get("dbtable"));
        jdbcDF = jdbcDF.select("*").where(jdbcDF.col("exchanges").isNotNull());


        RDD h = jdbcDF.rdd();
        //JavaRDD gg=jdbcDF.toJavaRDD();
        Row[] rows = jdbcDF.collect();
        List<String> data = new LinkedList<>();
        for (Row row2 : rows) {
            data.add(row2.toString());
        }

        JavaRDD<String> distData = sc.parallelize(data);
        DataFrame df = sqlContext.read().json(distData);
        //DataFrame jdbcDF2=sqlContext.read().json(h);
        // DataFrame jdbcDF2= sqlContext.read().json(jdbcDF.select("*").where(jdbcDF.col("exchanges").isNotNull()));
       /* System.out.println("Data------------------->" + jdbcDF.toJSON().first());
        Row[] rows = jdbcDF.collect();
        System.out.println("Without Filter \n ------------------------------------------------- ");
        for (Row row2 : rows) {
            System.out.println(row2.toString());
        }*/
        DataFrame personPositions = df.select(df.col("stats").as("stat"),
                org.apache.spark.sql.functions.explode(df.col("stats")).as("stat1"));

        DataFrame test = personPositions.select(
                personPositions.col("stat1").getField("accounts_created").as("accounts"), personPositions.col("stat1").getField("exchanges_count").as("exchanges"), personPositions.col("stat1").getField("ledger_count").as("ledger"), personPositions.col("stat1").getField("payments_count").as("payments"), personPositions.col("stat1").getField("date").as("date"));
        test.show();
        //System.out.println(TimeSeries.creatSeries(test).returnRates());
        //System.out.println(TimeSeries.creatSeries(test).seriesStats());
        // DataFrame idf=
        test.drop("ledger");

        test.col("accounts").cast("int");
        test.col("exchanges").cast("int");
        //  idf.col("ledger").cast("String");
        test.col("payments").cast("int");
        test.col("date").cast("String");

        DataFrame df1 = test.withColumnRenamed("accounts", "oldaccounts").withColumnRenamed("exchanges", "oldexchanges")
                .withColumnRenamed("payments", "oldpayments").withColumnRenamed("date", "olddate");
        DataFrame df2 = df1.withColumn("accounts", df1.col("oldaccounts").cast("int")).drop("oldaccounts")
                .withColumn("exchanges", df1.col("oldexchanges").cast("int")).drop("oldexchanges")
                .withColumn("payments", df1.col("oldpayments").cast("int")).drop("oldpayments")
                .withColumn("date", df1.col("olddate").cast("String")).drop("olddate");
        DataFrame cast1 = TimeSeries.creatSeries(df2, sc).toObservationsDataFrame(sqlContext, "date", "exchanges", "payments");

        cast1.drop("exchanges");
        //  idf.col("ledger").cast("String");
        cast1.col("payments").cast("String");

        cast1.col("date").cast("String");

        DataFrame dfc1 = cast1.withColumnRenamed("payments", "oldpayments").withColumnRenamed("date", "olddate");
        DataFrame dfc2 = dfc1.withColumn("payments", dfc1.col("oldpayments").cast("String")).drop("oldpayments")
                .withColumn("date", dfc1.col("olddate").cast("String")).drop("olddate");
        dfc2.toJSON();
        //  dfc2.createJDBCTable("jdbc:mysql://localhost:3306/demo?autoReconnect=true&useSSL=false&user=root&password=Scorpio21*","new",true);
        //.toInstantsDataFrame(sqlContext)
/*
        idf.drop("ledger");

        idf.col("accounts").cast("int");
        idf.col("exchanges").cast("Long");
      //  idf.col("ledger").cast("String");
        idf.col("payments").cast("Long");
        idf.col("date").cast("String");

        DataFrame df1 =idf.withColumnRenamed("accounts","oldaccounts").withColumnRenamed("exchanges","oldexchanges")
                .withColumnRenamed("payments","oldpayments").withColumnRenamed("date","olddate");
        DataFrame df2=df1.withColumn("accounts",df1.col("oldaccounts").cast("int")).drop("oldaccounts")
                .withColumn("exchanges",df1.col("oldexchanges").cast("Long")).drop("oldexchanges")
                .withColumn("payments",df1.col("oldpayments").cast("Long")).drop("oldpayments")
                .withColumn("date",df1.col("olddate").cast("String")).drop("olddate");
        df2.show();

*/
/*

        jdbcDF.drop("count").drop("result").drop("rate").drop("marker").show();
        jdbcDF.drop("count").drop("result").drop("rate").drop("marker").schema();


        df.drop("count").drop("marker").drop("rate").drop("result").show();
        df.drop("count").drop("marker").drop("rate").drop("result").schema();

        //df.drop("count").drop("marker").drop("rate").drop("result").rdd().toJavaRDD();
//MAYBE THIS SHIT WOULD BE A HELLUVALOT EASIER WITH D
        JavaRDD<Vector> vectors = test.javaRDD().map(new Function<Row, Vector>() {
            @Override
            public Vector call(Row row) throws Exception {
                // todo: select whichever fields from the sessionsAgg that should be included in the vectors
                //long a = row.getLong(row.fieldIndex("accounts"));
                long e = row.getLong(row.fieldIndex("exchanges"));
//                long l = row.getLong(row.fieldIndex("ledger"));
                long p = row.getLong(row.fieldIndex("payments"));
                //String d = row.getString(row.fieldIndex("date"));
                return Vectors.dense(new double[]{ e, p});
            }
        });

        System.out.println(Statistics.colStats(vectors.rdd()));
        System.out.println(Statistics.corr(vectors.rdd(), "pearson"));
*/
        // JavaRDD<Vector> vectors = df.drop("count").drop("marker").drop("rate").drop("result").javaRDD().map(new Function<Row, Vector>());
        //Row[] statist=jdbcDF.select("stats").collect();


        // System.out.println(statist.toString());
        /*
        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = org.apache.spark.mllib.stat.Statistics.colStats(distData.rdd());
        System.out.println(summary.mean()); // a dense vector containing the mean value for each column
        System.out.println(summary.variance()); // column-wise variance
        System.out.println(summary.numNonzeros()); // number of nonzeros in each column*/
        //jdbcDF2.show();
       /* System.out.println("Filter Data\n ------------------------------------------------- ");
        jdbcDF = jdbcDF.select("*").where(jdbcDF.col("exchanges").isNotNull());
        Row[] rows = jdbcDF.collect();
        rows = jdbcDF.collect();
        for (Row row2 : rows) {
            System.out.println(row2.toString());
        }*/
        try {
            if (statement != null)
                statement.close();
        } catch (SQLException e) {
            e.getErrorCode();
        }
        try {
            if (myConn != null)
                myConn.close();
        } catch (SQLException p) {
            p.getErrorCode();
        }
//            Json2SparkSQL test= new Json2SparkSQL();
        //     test.dataFrame();
        //call the method from the pojo
        //System.out.println(Arrays.deepToString(RippleExchange.toPojo(jsonObject,objType).getStats()));
        //------System.out.println(Arrays.deepToString(RippleExchange.toPojo(jsonObject,objType).getStats()));

        /*
        //gateways at https://ripple.com/knowledge_center/gateway-information/

        //specify currencies with gateways
        //    String base ="XRP";
        // String counter ="EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
        //to default to current date, remove date variable
        //   String date="2015-11-13T00:00:00Z";
        double amount=1;
        String currency="EUR";
       // String issuer="rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
        String issuer="rBycsjqxD8RVZP5zrrndiVtJwht7Z457A8";

        String exchange_currency="XRP";


        //create the link to the URL
        URL rippleNormal = new URL("https://data.ripple.com/v2/normalize?amount="+amount+"&currency="+currency+"&exchange_currency="+exchange_currency+"&issuer="+issuer);

        // buffering characters so as to provide for the efficient reading of characters, arrays, and lines
        BufferedReader br1 = new BufferedReader(new InputStreamReader(rippleNormal.openStream()));

        String inputLine1;
        while((inputLine1=br1.readLine()) !=null)
            System.out.println(inputLine1);


        //GET EXCHANGES
        String start="2016-01-01T00:00:00Z";
        String end="2016-01-02T00:00:00Z";
        //cannot be more than 1000 unless reduce is set to true
        int limit=100;
        Boolean reduce=true;
        Boolean descending=true;
        //create the link to the URL
        URL rippleGetExchanges = new URL("https://data.ripple.com/v2/exchanges/"+base+"/"+counter+"?"+descending+"&"+reduce+"&"+"limit="+limit+"&start="+start+"&end="+end);

        // buffering characters so as to provide for the efficient reading of characters, arrays, and lines
        BufferedReader br2 = new BufferedReader(new InputStreamReader(rippleGetExchanges.openStream()));

        String inputLine2;
        StringBuilder build = new StringBuilder();
        while((inputLine2=br2.readLine()) !=null) {
            build.append(inputLine2);
        }
        System.out.println(build);

        //serialistion

        //gson.toJson(inputLine);

        ExchangeRates ex1 = gson.fromJson(jsonObject, ExchangeRates.class);

        //ExchangeRates ex1 = gson.fromJson(br, ExchangeRates.class);

        System.out.println(ex1.getRate());
       // JSONObject json = new JSONObject(build.toString());

       /* //Bitcoin
        //gateways at https://ripple.com/knowledge_center/gateway-information/

        //specify currencies with gateways
        //    String base ="XRP";
        // String counter ="EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
        //to default to current date, remove date variable
        //   String date="2015-11-13T00:00:00Z";
        double amount=10;
        String currency="EUR";
        double value=1;

        // String exchange_issuer;

        //create the link to the URL
        URL bitUrl = new URL("https://data.ripple.com/v2/normalize?amount="+amount+"&currency="+currency+"&exchange_currency="+exchange_currency+"&issuer="+issuer);

        // buffering characters so as to provide for the efficient reading of characters, arrays, and lines
        BufferedReader br1 = new BufferedReader(new InputStreamReader(rippleNormal.openStream()));

        String inputLine1;
        while((inputLine1=br1.readLine()) !=null)
            System.out.println(inputLine1);*/
    }/*catch(JsonFromURL e) {

    }*/

    public JsonObject getJson(){
    return jsonObject;
}

}

