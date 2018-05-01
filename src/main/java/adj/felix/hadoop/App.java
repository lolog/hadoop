package adj.felix.hadoop;

import java.util.Calendar;
import java.util.Date;

public class App 
{
    public static void main( String[] args )
    {
    	Calendar calendar=Calendar.getInstance();
    	calendar.setTime(new Date());
    	System.out.println(calendar.get(Calendar.MONTH));
    }
}
