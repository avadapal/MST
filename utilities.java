package MST;

import java.util.ArrayList;
import java.util.List;

public class utilities {
	
	public static String[] split_new(String inString, String splitter)
    {
       List<String> outList = new ArrayList<String>();
       String[]     test    = inString.split(splitter);

       for(String s : test)
       {
           if(s != null && s.length() > 0)
               outList.add(s);
       }

       String[] res = new String[outList.size()];
       res = outList.toArray(res);
       return res;
    }
	
	static double compute_distance(String x, String y){
		
		String[] x1 = x.split(" ");
		String[] x2 = y.split(" ");
		
		Double distance  = 0.0;
		for(Integer i = 0; i < x1.length; i++){
		 	
			double x1_i = Double.parseDouble(x1[i]);
			double x2_i = Double.parseDouble(x2[i]);
			
			distance += (x1_i - x2_i) * (x1_i - x2_i);
		}
		 
	    distance = Math.sqrt(distance);	
		return distance;
	}

}
