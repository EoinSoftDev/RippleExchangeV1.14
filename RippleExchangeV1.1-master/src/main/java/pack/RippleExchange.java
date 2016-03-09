package pack;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

//C:\Users\10318411.IT2_DOMAIN.000\IdeaProjects\RippleExchange.out.production.RippleExchange.com.example.google.gson
/**
 * Created by 10318411 on 26/01/2016.
 */
//C:\Users\10318411.IT2_DOMAIN.000\IdeaProjects\RippleExchange out.production.RippleExchange.com.example.google
//https://data.ripple.com/v2/exchange_rates/?/???
public class RippleExchange {

   // public static final String apiMethod = "exchange_rates";
  //  public static final String base = "XRP";
  //  public static final String counter = "EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq";
    //to default to current date, remove date variable
 //   public static final String date = "2015-01-13T19:57:00Z";

    //https://data.ripple.com/v2/exchange_rates/XRP/EUR+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq/2015-01-13T19:57:00Z


    //method to return the string from the url
    public static String urlJsonString(URL ripple) throws IOException {
        // buffering characters so as to provide for the efficient reading of characters, arrays, and lines
        BufferedReader br = new BufferedReader(new InputStreamReader(ripple.openStream()));

        StringBuilder jsonString = new StringBuilder();
        String inputLine;
        while ((inputLine = br.readLine()) != null) {
            jsonString.append(inputLine);
        }
        return String.valueOf(jsonString);
    }
    //parse the string to a json object
    public static JsonObject jsonParse(String jsonString) {
        System.out.println(jsonString);
        JsonObject jsonObject = (new JsonParser()).parse(jsonString).getAsJsonObject();
        return jsonObject;
    }

 //CHANGE TO ADD ANOTHER ARGUEMNET FOR TYPE
 public static ApiMethod toPojo(JsonObject jsonObject,ApiMethod objType) {
     Gson gson = new Gson();

     ApiMethod ex1 = gson.fromJson(jsonObject,objType.getClass());
     return ex1;
 }
/*
    //return a pojo of exchangeRates from the json using gson api
    public static ExchangeRates toExchangeRatesPojo(JsonObject jsonObject) {
        Gson gson = new Gson();

        ExchangeRates ex1 = gson.fromJson(jsonObject, ExchangeRates.class);
        return ex1;
    }
*/
}
