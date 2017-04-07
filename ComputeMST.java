package MST;

import scala.Tuple2; 




import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.FlatMapFunction; 
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction; 
import org.apache.spark.api.java.function.Function2; 
import java.util.Arrays; 
import java.util.List; 
import java.util.ArrayList; 
import java.util.Collections; 
import org.apache.spark.api.java.function.Function; 

import java.util.Map; 
import java.util.TreeMap; 
import java.util.SortedMap; 
import java.util.Iterator; 

public class ComputeMST {
	
	public static void main(String[] args){
		
	      if (args.length < 2) { 
	          System.err.println("Usage: Top10 <input-path> <topN>"); 
	          System.exit(1); 
	       } 			
		

	    System.out.println("Compute Minimum Spanning Tree");
	    
	    List<Integer> MSTedges = new ArrayList<Integer>();
		
	    SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("local");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
		System.out.println("Collection being done!");
		
		JavaRDD<String> lines = ctx.textFile("/home/avadapal/workspace/myFirstSpark/src/MST/input.txt", 1); 
	    
		JavaPairRDD<Integer, Integer> ed = lines.mapToPair(new PairFunction<String, Integer, Integer>()
				{
                   public Tuple2<Integer, Integer> call(String s){
                	   String[] tokens  = s.split(" ");
                	   return new Tuple2<Integer, Integer>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
                   }
				}
		
		); 
		   
		System.out.println("Work Done!!!");   
		   List<Tuple2<Integer,Integer>> debug1 = ed.collect(); 
		      for (Tuple2<Integer,Integer> t2 : debug1) { 
		         System.out.println("key="+t2._1 + "\t value= " + t2._2); 
		      }
		      
	
	JavaPairRDD<Integer, String> ed1 = lines.mapToPair(new PairFunction<String, Integer, String>()
			{
		      public Tuple2<Integer, String> call(String s){
		    	  String[] tokens = s.split(" ");
		    	  return new Tuple2<Integer, String>(Integer.parseInt(tokens[1]), s);
		      }
			}	
	);	   
	
	JavaPairRDD<Integer, Integer> ed2 = lines.mapToPair(new PairFunction<String, Integer, Integer>()
			{
		      public Tuple2<Integer, Integer> call(String s){
		    	  String[] tokens = s.split(" ");
		    	  
		    	  Integer Key = 0;
		    	  Integer val1 = Integer.parseInt(tokens[1]);
		    	  
		    	  if(val1 <= 10){
		    		  Key = 1;
		    	  }
		    	  else{
		    	     Key = 2;
		    	  }
		    	  
		    	  return new Tuple2<Integer, Integer>(Key, Integer.parseInt(tokens[1]));
		      }
			}	
	);	    
	
	
	JavaPairRDD<Integer, Integer> result =
		ed2.reduceByKey(new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer i1, Integer i2) {
		return i1 + i2;
		}
		});
	
	
	JavaPairRDD<Integer, Integer> ed3 = lines.mapToPair(new PairFunction<String, Integer, Integer>()
			{
		      public Tuple2<Integer, Integer> call(String s){
		    	  String[] tokens = s.split(" ");
		    	  
		    	  Integer Key = 0;
		    	  Integer val1 = Integer.parseInt(tokens[1]);
		    	  
		    	  if(val1 <= 10){
		    		  Key = 1;
		    	  }
		    	  else{
		    	     Key = 2;
		    	  }
		    	  
		    	  return new Tuple2<Integer, Integer>(Key, Integer.parseInt(tokens[1]));
		      }
			}	
	);	
	

	
	
	System.out.println("ccc");
	
	JavaPairRDD<Integer, Iterable<Integer>> result1 = ed2.groupByKey();
		
	System.out.println(result1.collect());
	System.out.println("Result1 print");
	
	JavaPairRDD<Integer, ArrayList<Integer>> result2 = result1.mapValues(new Function<Iterable<Integer>, ArrayList<Integer>>(){
	
		//Iterator animalItr = animal.iterator();
		@Override
		public ArrayList<Integer> call(Iterable<Integer> s) throws Exception {
			// TODO Auto-generated method stub
			
			ArrayList<Integer> ss = new ArrayList();

			List<Integer> list  = new ArrayList<Integer>();
			List<Integer> list1 = new ArrayList<Integer>();
			
			for(Integer aa: s){
				list.add(aa);
			}
			
			for(Integer i = 1; i < list.size(); i++)
			{
				for(Integer j = i; j < list.size(); j++)
				{
				Integer dist = list.get(j) - list.get(j-1); 
				list1.add(dist);
				}
			}
			
			for(Integer a: list1){
			ss.add(a);      
			}
			return ss;
		}
		
	});
	
	
	System.out.println(result2.collect());
	System.out.println("Result 2 print");
	System.out.println(ed1.collect());
	
	System.out.println(ed2.collect());
	System.out.println("Ed2 collect");
	
	System.out.println(result.collect());
	
	System.out.println("End of the Program");

	}

}
