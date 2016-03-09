package pack;

import com.google.gson.JsonObject;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

/**
 * Created by 10318411 on 03/02/2016.
 */

public class RippleMain {
    public JsonObject jsonObject;
    public JsonObject getJson(){
    return jsonObject;
}

    public static void main(String[] args) throws IOException /*try*/{

        String base ="XRP";
        String counter ="EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
        //to default to current date, remove date variable
        String date="2015-01-13T19:57:00Z";
 ;      String apiMethod = "stats";
        //protected String apiMethod = "exchange_rates";

        //select which api method you want to retrieve
        URL rippleUrl=null;
        ApiMethod objType=null;
        switch (apiMethod) {
            //create the link to the URL
            case ("exchange_rates"):
                rippleUrl = new URL("https://data.ripple.com/v2/" + apiMethod + "/" + base + "/" + counter + "?" + date);
                System.out.println("1");
                objType= new ExchangeRates();
                break;
            case ("stats"):
                rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=2015-08-30&end=2015-08-31&interval=hour&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());
                System.out.println("2");
                objType=new Stats();
                break;

            default: System.out.println("no url");

        }
        System.out.println(rippleUrl);

        //parsing the json from the url
       JsonObject jsonObject = RippleExchange.jsonParse(RippleExchange.urlJsonString((rippleUrl)));

        FileWriter file = new FileWriter("/home/eoin/Documents/JsonFiles/file1.txt");
        try{
            file.write(String.valueOf(jsonObject));
            System.out.println("Sucessful copy");
            System.out.println("\nJSOn : "+jsonObject);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            file.flush();
            file.close();
        }

        Json2SparkSQL test= new Json2SparkSQL();
       test.dataFrame();
        //call the method from the pojo
        //System.out.println(Arrays.deepToString(RippleExchange.toPojo(jsonObject,objType).getStats()));
        System.out.println(Arrays.deepToString(RippleExchange.toPojo(jsonObject,objType).getStats()));
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

}

