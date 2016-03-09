package pack;

import java.lang.reflect.Array;

//class to store json exchange rates as pojo's
    //from "https://data.ripple.com/v2/exchange_rates/
public class ExchangeRates extends ApiMethod {

    private String result;
    private String rate;

    public void setResult(String result)
    {
        this.result=result;
    }

    public String getResult()
    {
        return result;
    }

    @Override
    public Array[] getStats() {
        return new Array[0];
    }

    public void setRate(String rate)
    {
        this.rate=rate;
    }

    public String getRate()
    {
        return rate;
    }
}
