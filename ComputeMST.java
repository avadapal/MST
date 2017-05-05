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

//import io.netty.util.ResourceLeakDetector.Level;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays; 
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List; 
import java.util.ArrayList; 
import java.util.Collections; 
import java.util.Random;

import org.apache.spark.api.java.function.Function; 

import com.clearspring.analytics.util.Pair;

import java.util.Map; 
import java.util.TreeMap; 
import java.util.SortedMap; 
import java.util.Iterator; 
//import java.util.logging.Logger;

import org.apache.log4j.Logger;
import org.apache.log4j.*;
import javolution.io.Struct.Bool;

public class ComputeMST {
	

	
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
    


    static String find_min_dist(ArrayList<String> a1, ArrayList<String> a2){
    	
    	double Min = Double.MAX_VALUE;
    	String new_edge = "";
    	for(int i = 0; i < a1.size(); i++){
    		String s1 = a1.get(i);
    		String cords1[] = s1.split(",");
     		for(int j = i; j < a2.size(); j++){
     			String s2 = a2.get(j);
     			String cords2[] = s2.split(",");
    		
    			double dist1 = compute_distance(cords1[0], cords2[1]);
    			if(dist1 < Min){
    				Min = dist1;
    				new_edge = cords1[0] + "," + cords2[1];
    			}
    			double dist2 = compute_distance(cords1[0], cords2[0]);
    			if(dist2 < Min){
    				Min = dist2;
    				new_edge = cords1[0] + "," + cords2[0];
    			}
    			double dist3 = compute_distance(cords1[1], cords2[0]);
    			if(dist3 < Min){
    				Min = dist3;
    				new_edge = cords1[1] + "," + cords2[0];
    			}
    			double dist4 = compute_distance(cords1[1], cords2[1]);
    			if(dist4 < Min){
    				Min = dist4;
    				new_edge = cords1[1] + "," + cords2[1];
    			}
    		}
    		
    	}
    	
    	
    	
    	return new_edge + "_" + Min;
    	
    }
    
    static double combine_connected_components(List<String> s){
    	System.out.println("Combine Connected Components NEW");
    	Map<Integer, ArrayList<String>> CC = new HashMap<Integer, ArrayList<String>>();
    	Integer Key = 0;
    	for(String ss: s){
    		String toks[] = split_new(ss, ";");
    		ArrayList<String> cc = new ArrayList<String>(Arrays.asList(toks));
    		CC.put(Key, cc);
    		Key++;
    	}
    	
    
    	
    	double len = 0.0;

    
   int initial_size = CC.size(); 	
   double[][] min_distances = new double[CC.size()][CC.size()];

   while(true)
    {
    	int x = 0;
        int y = 0;
        	
        double min = Double.MAX_VALUE;
    	for(Integer key1 : CC.keySet()){  		
    		for(Integer key2 : CC.keySet()){
    			if(key1 == key2){
    				min_distances[key1][key2] = 0.0;
    				continue;
    			}

    			ArrayList<String> a1 = CC.get(key1);
    			ArrayList<String> a2 = CC.get(key2);
    			
    			
    			double min_cur = 0.0;
    			if(min_distances[key1][key2] != 0){
    				min_cur = min_distances[key1][key2];
    			}
    			else{
    			String edge_min = find_min_dist(a1, a2);
    			String edMin[] = edge_min.split("_");
    			
    			min_cur = Double.parseDouble(edMin[1]);
    			}
    			min_distances[key1][key2] = min_cur;
    			min_distances[key2][key1] = min_cur;
    			if( min_cur < min)
    			{
    				min = min_cur;
    				x = key1;
    				y = key2;
    			}
    			
    		} 		
    	}

    	ArrayList<String> a1 = CC.get(x);
    	ArrayList<String> a2 = CC.get(y);
    	a1.addAll(a2);
    	
    	len += min;

    	CC.put(x, a1);

    	CC.remove(y);

    	for(int i = 0; i < initial_size; i++){
    		
    		if(min_distances[y][i] < min_distances[x][i]){
    			min_distances[x][i] = min_distances[y][i];
    			min_distances[i][x] = min_distances[i][y];
    			min_distances[x][y] = 0.0;
    			min_distances[y][x] = 0.0;
    		}
    	}
    	
    	if(CC.size() == 1) break;
    }
    	return len;
    }
    
    

    

    
    
    static double Prim(String[] points){
    	
    	Integer n = points.length;
    	Double length = 0.0;
    	
    	if(n == 0){
    		return 0.0;
    	}
    	
    	Double[] dist = new Double[n];
    	boolean[] done =  new boolean[n];
    	String[] vertex = new String[n];
    	for(int i = 0; i < n ; i++){
    		done[i] = false;
    		dist[i] = Double.MAX_VALUE; 		
    	}
    	
    	dist[0] = 0.0;
    	String prim_mst = "";
        for (int i = 0; i < n; i++) {
            Integer best = -1;
            Double bestdist = Double.MAX_VALUE;
            String edge_added = "";
            for (int j = 0; j < n; j++) {
              if (!done[j] && dist[j] < bestdist) {
                best = j;
                bestdist = dist[j];
                if(bestdist != 0) edge_added = points[best] + "," + vertex[best];
                
              }
            }
            length += bestdist;
            prim_mst = prim_mst + edge_added + ";";
            //System.out.println("bestdistance added = " + bestdist);
            //System.out.println("edge_added = " + edge_added);
            //System.out.println("bestdist = " + bestdist);
            done[best] = true;
            for (int j = 0; j < n; j++) {
              if (!done[j]&& dist[j] > compute_distance(points[best], points[j])) {
            	  //System.out.println(points[best]  + " ," + points[j]);
                dist[j] = compute_distance(points[best], points[j]);
                vertex[j] = points[best]; 
                //System.out.println("vertex[j] = " + vertex[j] );
                //System.out.println("distance = " + dist[j]);
                
                
              }
            }
          }
    	
    	System.out.println("length = " + length);
    	System.out.println("primMST = " + prim_mst);
    	
    	return length;
    	//return prim_mst;
    }
    
  
    
    public static String PrimShort(String[] points, double maxLength){
      //  System.out.println("Entering PrimShort... maxLength = " + maxLength);
           
         
       // for (Point p : points) {
        //  System.out.println(p.print());
       // }
        String short_mst = "";
        String short_mst1 = "";
        double length = 0;
        int n = points.length;
        boolean[] done = new boolean[n]; 
        if (n == 0) {
            System.out.println("Terminating Prim since n = 0.");
            return null;
        }
     
        HashSet<Integer> conn = new HashSet<Integer>();
        Double[] dist = new Double[n];
        String[] vertex = new String[n];
        Integer[] color = new Integer[n];
        
        for(int itt = 0 ; itt < n; itt++){
        	color[itt] = itt;
        	done[itt] = false;
        }
        
        for (int it = 0; it < n; it++) {
            Integer curcolor = color[it];
            if (!conn.contains(curcolor)) {
                conn.add(curcolor);
            } else {
                continue;
            }
             
            for (int i = 0; i < n; i++) {
                  dist[i] = Double.MAX_VALUE;
            }
             
            dist[it] = 0.0;
            int best = it;
             
          //  System.out.println("Entering loop...");
       
            while (true) {  
                String bestPoint = points[best];
                long oldColor = color[best];
                int node2 = 0;
                for (int i = 0; i < n; i++) {
                  String curPoint = points[i];
                  if (color[i] == oldColor) {
                      color[i] = curcolor;
                      for (int j = 0; j < n; j++) {
                          if (dist[j] > compute_distance(curPoint, points[j])) { 
                              dist[j] = compute_distance(curPoint, points[j]);                          
                              vertex[j] = curPoint; // points[best];
                              node2 = i;
                          }  
                      }
                    
                  }          
                }
                
            //  System.out.println("short mst intermediate = " + short_mst1);
            //  System.out.println("Old color = " + oldColor);  
                 
              best = -1;
              double bestdist = Double.MAX_VALUE;
              int node = 0;
              String shortmst1 = "";
              for (int i = 0; i < n; i++) {
                  String next = points[i]; 
                  if (curcolor != color[i] && dist[i] < bestdist && dist[i] <= maxLength) {
                      best = i;
                      bestdist = dist[i];
                      node = i;
                      if(bestdist != 0.0){
                      
                      }
                  }
              }
              
             
              
            //  System.out.println("Best = " + best);
              if(bestdist == Double.MAX_VALUE){
            	  short_mst1 = short_mst1 + "C";
            	  //short_mst1 = "";
              }
              //System.out.println("Best dist = " + bestdist);
              if (best == -1) {
                  break;
              }
              length += bestdist;
              short_mst = short_mst + ";" + points[node] + "," + vertex[node];
              assert(points[node2] == vertex[node]);
              done[node2] = true;
              done[node] =  true;
              short_mst1 = short_mst1 + ";" + points[node] + "," + vertex[node];
              //System.out.println("length added = " + bestdist);
            }      
             
            //System.out.println("Exiting loop...");
        }
        
        for(int i = 0; i < n; i++){
        	if(done[i] == false){
        		short_mst1 = short_mst1 + points[i] + "," + points[i] + ";C";
        	}
        }

        assert(length < n);
        

        return short_mst1 +"l" + Double.toString(length);
      } 
    
    
 public static String[] epsNet(ArrayList<String> points , double distance) {
//  	  System.out.println("Entering epsNet...");
  	  
  	  int n = points.size();
  	  double [] dist = new double[n];
  	  for (int i = 0; i < n; i++) {
  		  dist[i] = Integer.MAX_VALUE;
  	  }
  	  
  	  String[] result = new String[n];
  	  ArrayList<String> res = new ArrayList();
  	 // System.out.println("n = " + n);
  	  
  	  int k = 0;
  	  while (true) {
  		  double maxdist = 0;
  		  int furthest = 0;
  		  for (int i = 0; i < n; i++) {
  			  if (dist[i] > maxdist) {
  				  maxdist = dist[i];
  				  furthest = i;
  			  }
  		  }
  		  if (maxdist <= distance) {
  			  break;
  		  }
  		  if(points.get(furthest) != null){
  			  result[k] = points.get(furthest);
  			  res.add(result[k]);
  			  k++;
  		  }
  		  
  		  //result.add(points[furthest]);
  		  for (int i = 0; i < n; i++) {
  			  dist[i] = Math.min(dist[i], compute_distance(points.get(furthest), points.get(i))); //Point.dist(points.get(furthest), points.get(i)));
  		  }
  	  }

  	  String[] myArray = res.toArray(new String[res.size()]);
  	  return myArray;
    }

    


    
	// Computes distance between two points "x" and "y" such x , y \in \mathhbb{R^d}
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
	
	


	
	
	public static double seq_MST(String input) throws IOException{
		
		BufferedReader reader = new BufferedReader(new FileReader(input));
		
		int nlines1 = 0;
	
		while (true){
			
	       String t = reader.readLine();
	       
	       //System.out.println("t = " + t);
	        if(t == null){	        	//System.out.println("t = " + t);
	        	break;
	        }
	        
			nlines1++;
			
		}
		
		String[] points = new String[nlines1];
		
		
		
		reader.close();
		
	
		BufferedReader reader1 = new BufferedReader(new FileReader(input));
		
		int n = 0;
		for (n = 0; n < nlines1; n++){
			
	       String t = reader1.readLine();
	    
	       String[] tokens = t.split(";");
	       
	       points[n] = tokens[1];
	       

		}
		
		reader1.close();
		
		double l = Prim(points);

		
		return l;
	}
	
	public static void main(String[] args) throws IOException{
	
		Logger.getLogger("org").setLevel(Level.OFF); 
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		FileWriter fw = new FileWriter("/home/avadapal/workspace/myFirstSpark/src/MST/input1.txt");
		
		Random rand = new Random();
		
		for(int i = 1; i <= 300; i++)
		{
			int  n1 = rand.nextInt(100) + 1;
			int  n2 = rand.nextInt(100) + 1;
			int  n3 = rand.nextInt(100) + 1;
			int  n4 = rand.nextInt(100) + 1;
			int  n5 = rand.nextInt(100) + 1;
			int  n6 = rand.nextInt(100) + 1;
			int  n7 = rand.nextInt(100) + 1;
			int  n8 = rand.nextInt(100) + 1;
			String s = i +";"+ n1 + " " + n2 + " " + n3 + " " + n4; // + " " + n5 + " " + n6 + " " + n7 + " " + n8;
			//System.out.println("s = " + s);
			fw.write(s + '\n');
             		
		}
        fw.close();
        
        String inputPath = "/home/avadapal/workspace/myFirstSpark/src/MST/input1.txt";
        
		BufferedReader reader = new BufferedReader(new FileReader(inputPath));
		int nlines1 = 0;
		int max = 0;
		while (true){
			
	       String t = reader.readLine();
	       
	       //System.out.println("t = " + t);
	        if(t != null)
	        {    
	        String linesplit[] = t.split(";");
	       
	        String cordsplit[] = linesplit[1].split(" ");
	        for(String a: cordsplit){
	        	if(Integer.parseInt(a) > max){
	        		max = Integer.parseInt(a);
	        	}
	         }
		    }
	        else{
	        	break;
	        }
	        
			nlines1++;
			
		}
		
		reader.close();
		final int nlines = nlines1;
		final int Delta = max + 1;
		final double eps = 0.20;
		final int d = 4;
		System.out.println("Delta = " + Delta);
		long startTime_dis = System.nanoTime(); 
		System.out.println("Compute Minimum Spanning Tree");
	    System.out.println("lines = " + nlines);
	    SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("local[4]").set("spark.executor.memory","1g");		
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);	    
		System.out.println("Collection being done!");

		JavaRDD<String> lines = ctx.textFile(inputPath, 16); 
	    
		
		JavaPairRDD<Integer, String> heirarchical_clusters = lines.mapToPair(new PairFunction<String, Integer, String>()
				{
              public Tuple2<Integer, String> call(String s){
         	   String[] tokens  = s.split(";");
         	  String[] coordinates = tokens[1].split(" ");
         	  
         	  Integer heirarchichal_break = Delta/2;
    
         	  String bin = "";
         	  
         	  for(String a: coordinates){
         		  
         		  Integer x = Integer.parseInt(a);
         		
         		  if(x > heirarchichal_break)
         		  {
         		  bin = bin + "1";
         		  }
         		  else
         		  {
         			bin = bin + "0";  
         		  }
         	  }
         	  Integer key = Integer.parseInt(bin, 2) + 1;
         	     	   
         	   return new Tuple2<Integer, String>(key, s);
            }
			}
	
	); 
		
	JavaPairRDD<Integer, Iterable<String>> h_grp = heirarchical_clusters.groupByKey();	
	
	System.out.println("Heirarchical Clusters ... ");
	System.out.println(h_grp.collect());
	
	JavaPairRDD<Integer, String> h_grp1 = h_grp.mapValues(new Function<Iterable<String>, String>() {
	public String call(Iterable<String> s) {
    	
         Integer no_of_ver =0;
		 Iterator<String> s1 = s.iterator();
         while (s1.hasNext()) {
		  s1.next();
		  no_of_ver++;
		}
	
		String[] cords = new String[no_of_ver];
		int i = 0;
	    for(String t: s){
	    	
	    	String tokens[] = t.split(";");
	    	cords[i] = tokens[1];
	    	i++;
	    }
	    

	
	 double Delta_l = Delta * 2.0 * Math.sqrt(d);
	 double eps2Delta = eps * eps * (Delta_l/2);
	 double epsDelta = eps * (Delta_l/2);

	String cc = PrimShort(cords, epsDelta);
	String[] toks = cc.split("l");
	String len = toks[1];
	
	String connected_comps = toks[0];

	
	HashMap<String, Integer> cc_node = new HashMap<String, Integer>();
	HashMap<Integer, String> node_cc = new HashMap<Integer, String>();
	Map<Integer, ArrayList<String>> multiValueMap = new HashMap<Integer, ArrayList<String>>();
	String[] ccs = split_new(connected_comps, "C");

	ArrayList<String> nodesInMSF = new ArrayList<String>();
	Integer Key = 0;
	
	for(String ss: ccs){
		ArrayList<String> nodesincc = new ArrayList<String>(); 
		String[] edges = split_new(ss , ";");
		for(String sss: edges){
		String[] nodes = split_new( sss , ",");
		for(String mm : nodes){
			cc_node.put(mm, Key);
			node_cc.put(Key,mm);
			nodesInMSF.add(mm);
			nodesincc.add(mm);
		}
		}
	    multiValueMap.put(Key, nodesincc);
		Key++;
	}
	
	
	String[] net1 = epsNet(nodesInMSF, eps2Delta);
	Map<Integer, ArrayList<String>> connected_comps_net = new HashMap<Integer, ArrayList<String>>();
	for(String nn1 :  net1){
		Integer key = cc_node.get(nn1);
		ArrayList<String> a = new ArrayList<String>();
		if(connected_comps_net.containsKey(key)){
		a = connected_comps_net.get(key);
		a.add(nn1);
		connected_comps_net.put(key, a);
		}
		else{
			a.add(nn1);
			connected_comps_net.put(key, a);
		}
	}
	
	
	String connected_compo_new ="";
	
	for(int key = 0; key < Key; key++){
		ArrayList<String> ccc = connected_comps_net.get(key);
		for(String r : ccc){
			connected_compo_new = connected_compo_new + r + "," + r + ";";
		}
		connected_compo_new += "C";
		
	}

	return connected_compo_new + "l" + len;

	}
	});
	
	
	System.out.println("h_grp1 below");
	System.out.println(h_grp1.collect());
	
	JavaRDD<String> values = h_grp1.values();	
	List<String> vals = values.collect();
	List<String> vals1 = new ArrayList<String>(); 
	
	double length = 0.0;
	
	for(String ss : vals){
		String[] tok = ss.split("l");
		String cc = tok[0];
	    length += Double.parseDouble(tok[1]);
		String[] tokens = cc.split("C");
		for(String sss: tokens){
			if(sss.length() > 0 && (sss != null || sss !="" || !sss.isEmpty()))	vals1.add(sss);	
		}
	}
	
	
	double len = combine_connected_components(vals1);
	long estimatedTime_dis = System.nanoTime() - startTime_dis;
	
	System.out.println("len = " + len);
	
	double mst_val = length + len;
	
	System.out.println("End of the Program");
	long startTime_seq = System.nanoTime();    
	// ... the code being measured ...  
	double len_reg_mst = seq_MST(inputPath);
	long estimatedTime_seq = System.nanoTime() - startTime_seq;
	
	System.out.println("time for seq MST = " + estimatedTime_seq/1000000000);
	System.out.println("time for Distr MST = " + estimatedTime_dis/1000000000);
	System.out.println("mst_val = " + mst_val + " and regular mst = " + len_reg_mst);

	}

}
