package pack;

import java.sql.Timestamp;
import java.util.ArrayList;


/**
 * Created by eoin on 10/04/16.
 */
public class Timestamps {

    // private static int count=0;
    private ArrayList TimeList;


    public Timestamps() {
        TimeList = new ArrayList<java.sql.Timestamp>();
    }

    /*  public Date getLastDate() {

          return TimeList.get(count);
      }
*/
    public ArrayList<java.sql.Timestamp> getTimeList() {
        return TimeList;
    }

    public void addDate(Timestamp date) {
        TimeList.add(date);
        //count++;
    }


    @Override
    public String toString() {
        String totString = "{";
        for (Object t : TimeList) {
            totString += "\"date\" : " + "\"" + t.toString() + "\", ";
        }
        return totString + "}";
    }

}
//yyyy-MM-dd'T'HH:mm:ss'Z'
//2016-01-01 00:00:00.0