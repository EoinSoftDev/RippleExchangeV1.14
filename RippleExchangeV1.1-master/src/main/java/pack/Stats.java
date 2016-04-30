package pack;

import java.lang.reflect.Array;

//Class to create  a POJO from Stats JSON
public class Stats extends ApiMethod{
    private String result;
    private Array stats[];
    //private Object exchanges_count;
    private int count;

    @Override
    public String getResult()
    {
        return result;
    }

    public void setResult(String result)
    {
        this.result=result;
    }

    public Array[] getStats()
    {
        return stats;
    }

    public void setStats(Array[] stats) {
        this.stats = stats;
    }

    public int getCount()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return String.valueOf(stats);
    }


}
