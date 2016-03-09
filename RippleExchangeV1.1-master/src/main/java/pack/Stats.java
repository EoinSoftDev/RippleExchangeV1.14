package pack;

import java.lang.reflect.Array;

/**
 * Created by 10318411 on 09/02/2016.
 */
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
    public void setStats(Array[] stats){
        this.stats=stats;
    }

    public Array[] getStats()
    {
        return stats;
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
