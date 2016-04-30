package pack;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

//handles forming the URLs to make requests to the JSON API
public class APIQueries {
    //Holds all the formed URLs
    static ArrayList<URL> URLList = new ArrayList<URL>();

    static URL rippleUrl = null;

    public static ArrayList createQueries(String apiMethod, String start, String end, String interval) throws IOException {
        try {
            //create dateTimes from string so we may iterate through dates
            DateTime d1;
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dt = formatter.parseDateTime(start);


            DateTime dt1 = formatter.parseDateTime(end);


            long diffInMillis = dt.getMillis() - dt1.getMillis();


            long differenceDates = diffInMillis / (24 * 60 * 60 * 1000);

            //Convert long to String
            String dayDifference = Long.toString(differenceDates);


            //we can only return approx 200 results from the API at a time
            //which amounts to approx 8 days worth of results
            //therefore we must break it up into 8 day segments
            if (differenceDates > 8) {
                DateTime end1 = null;
                for (int i = 0; i < differenceDates / 8; i++) {
                    if (i == 0) {
                        end1 = dt.minusDays(8);
                    }
                    start = dt.toString();
                    end = end1.toString();

                    //building our URL and adding it to an ArryaLIst for later use to populate the database
                    rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=" + start.substring(0, 10) + "&end=" + end.substring(0, 10) + "&interval=" + interval + "&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());

                    URLList.add(rippleUrl);
                    dt = dt.minusDays(8);
                    end1 = end1.minusDays(8);
                }
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DATE, -8);
                Date dateBefore8Days = cal.getTime();


                rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=" + start + "&end=" + end + "&interval=" + interval + "&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());

                //case where the interval is less than 8 days
            } else {

                rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=" + start + "&end=" + end + "&interval=" + interval + "&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());

                URLList.add(rippleUrl);
            }

        } catch (Exception e) {
            e.printStackTrace();

        }

        return URLList;
    }
}
