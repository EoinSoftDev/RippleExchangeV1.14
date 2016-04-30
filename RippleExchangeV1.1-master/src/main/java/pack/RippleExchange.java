package pack;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class RippleExchange {
    //method to return the string from the url request
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
        JsonObject jsonObject = (new JsonParser()).parse(jsonString).getAsJsonObject();
        return jsonObject;
    }

    //Can change and subclass of APIMethod to a POJO
    //Currently Stats and Exchange Rates but allows for extendability
 public static ApiMethod toPojo(JsonObject jsonObject,ApiMethod objType) {
     Gson gson = new Gson();

     ApiMethod ex1 = gson.fromJson(jsonObject,objType.getClass());
     return ex1;
 }

}
