package pack;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by eoin on 18/03/16.
 */
public class APIQueries {

    static ArrayList<URL> URLList = new ArrayList<URL>();
    //String start = "2016-03-15";
    //String end = "2014-08-31";
    //String interval = "hour";
    static URL rippleUrl = null;

    public static ArrayList createQueries(String apiMethod, String start, String end, String interval) throws IOException {
        try {

            //Dates to compare
            // String CurrentDate = "09/24/2015";
            //String FinalDate = "09/26/2015";
            DateTime d1;
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dt = formatter.parseDateTime(start);
            System.out.println(dt);
            //Date date1;
            //Date date2;

            DateTime dt1 = formatter.parseDateTime(end);


            long diffInMillis = dt.getMillis() - dt1.getMillis();


            long differenceDates = diffInMillis / (24 * 60 * 60 * 1000);

            //Convert long to String
            String dayDifference = Long.toString(differenceDates);
            System.out.println(dayDifference);

            // Date date3=dates.parse(start);
            if (differenceDates > 8) {
                DateTime end1 = null;
                for (int i = 0; i < differenceDates / 8; i++) {
                    if (i == 0) {
                        end1 = dt.minusDays(8);
                    }
                    start = dt.toString();
                    end = end1.toString();
                    System.out.println(start.substring(0, 10) + " , " + end.substring(0, 10));

                    rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=" + start.substring(0, 10) + "&end=" + end.substring(0, 10) + "&interval=" + interval + "&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());
                    // System.out.println(rippleUrl.toString());
                    URLList.add(rippleUrl);
                    dt = dt.minusDays(8);
                    end1 = end1.minusDays(8);
                }
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DATE, -8);
                Date dateBefore8Days = cal.getTime();


                rippleUrl = new URL(new StringBuilder().append("https://data.ripple.com/v2/").append(apiMethod).append("/").append("?start=" + start + "&end=" + end + "&interval=" + interval + "&family=metric&metrics=accounts_created,exchanges_count,ledger_count,payments_count").toString());


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