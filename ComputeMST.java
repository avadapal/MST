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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays; 
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

import javolution.io.Struct.Bool;

public class ComputeMST {
	
    // A utility function to find the vertex with minimum key
    // value, from the set of vertices not yet included in MST
    static int minKey(double[] key, Boolean mstSet[], Integer V)
    {
        // Initialize min value
        double min = Integer.MAX_VALUE;
        int min_index=-1;
 
        for (int v = 0; v < V; v++)
            if (mstSet[v] == false && key[v] < min)
            {
                min = key[v];
                min_index = v;
            }
 
        return min_index;
    }
	
    public static String[] split_new(String inString)
    {
       List<String> outList = new ArrayList<String>();
       String[]     test    = inString.split(";");

       for(String s : test)
       {
           if(s != null && s.length() > 0)
               outList.add(s);
       }

       String[] res = new String[outList.size()];
       res = outList.toArray(res);
       return res;
    }
    
    static String rNet(double r, String set){
    	
    	String cords[] = set.split(" ");
    	
    	
    	return null;
    }
    
    static String compute_min_dist_between_two_sets(String[] set1, String[] set2){
    	
    //	System.out.println("set1: ");
    	for(String ss: set1){
    		//System.out.println(ss);
    	}
    	
    	//System.out.println("set2: ");
    	for(String ss: set2){
    		//System.out.println(ss);
    	}
    	
    	double Min = Double.MAX_VALUE;
    	String new_edge = "";
    	for(String s1 : set1){
    		if(s1 == set1[0]) continue;
    		String cords1[] = s1.split(",");
     		for(String s2: set2){
     			String cords2[] = s2.split(",");
    			if(s2 == set2[0]) continue;
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
    
    
    static String find_min_distance_and_update_connected_components(String[][] sets, Integer size)
    {
    	for(int i = 0; i < size; i++){
    		//System.out.println("CCCCCC" + i + ":  " + sets[i][0]); 

    	}
    	System.out.println("find min distance and update connected components ...");
    	
    	double min = Double.MAX_VALUE;
    	String edge_added = "";
    	Integer c1 = 0;
    	Integer c2 = 0;
    	
    	System.out.println("size = " + size);
    	
    	for(int k = 0; k < size ; k++){	
    		int l = sets[k].length;
    		System.out.println("l = " + l);
        	for(int j = 0; j < size; j++){
        		System.out.println("j = " + j + " , " + " k = " + k);
        		System.out.println(" check ");
        		//System.out.println(sets[k][0] + "<->" + sets[j][0]);
        		if(sets[k][0] != sets[j][0]){
        		//	System.out.println(sets[k][0] + "<->" + sets[j][0]);	
        		String res = compute_min_dist_between_two_sets(sets[k], sets[j]);
        		String tok[] = res.split("_");
        		String new_edge = tok[0];
        		double cur_min = Double.parseDouble(tok[1]);
        		if(cur_min < min){
        			min = cur_min;
        			edge_added = new_edge; 
        			c1 = j;
        			c2 = k;
        		}
        		
        		}
        	}       	
        }
    	
    	System.out.println("out of loop ...");
    	
      	for(int i = 0; i < size; i++){
    		//System.out.println("bbbbb" + i + ":  " + sets[i][0]); 

    	}
    	
    	//System.out.println("c1 = " + c1 + " vl = " + sets[c1][0]);
    	
    	//System.out.println("c2 = " + c2 + "  vl = " + sets[c2][0]);
    	
    	Integer com_ind = Integer.parseInt(sets[c1][0]);
    	
    	for(int i = 0; i < size; i++){
    		System.out.println(i + ":  " + sets[i][0] + " < - > " + sets[c1][0]);
    		if(Integer.parseInt(sets[i][0]) == com_ind){
    			//System.out.println("iiii = " + i);
    			//System.out.println(sets[i][0] + " < - > " + sets[c1][0]);
    			sets[i][0] = sets[c2][0];
    		}
    	}
    	if(Integer.parseInt(sets[c1][0]) < Integer.parseInt(sets[c2][0])){
  //  		sets[c2][0] = sets[c1][0];
    	}
    	else{
//    		sets[c1][0] = sets[c2][0];
    	}
    
    	//System.out.println("c1 = " + c1 + " , " + "c2 = " + c2);
    	System.out.println("sets ...");
    	for(int j = 0; j < size; j++){
    		for(String rr: sets[j]){
    			System.out.println("rr = " + rr);
    		}
    		System.out.println("............................");
    	}
    	
    	return edge_added;
    	
    	
    }
    
    static String combine_connected_components(List<String> s){
    	
    	System.out.println("Combine Connected Components");
    	
    	Integer size = s.size();
    	System.out.println("size = " +  size);
    	String sets[][] = new String[size + 1][];
    	int i = 0;
    	int connected_comp = 0;
    	for(String ss: s){
    		//System.out.println("ss = " + ss);
    		ss = connected_comp + ";" + ss;
    		String toks[] = split_new(ss); //ss.split_new(";");
    		sets[i] = toks;
    		i++;
    		connected_comp++;
    	}
    	
 	
    	System.out.println("Some Debugging");

    	System.out.println("sets ...");
    	for(int j = 0; j < size; j++){
    		for(String rr: sets[j]){
    			System.out.println("rr = " + rr);
    		}
    		System.out.println("............................");
    	}
    	
    	
    	for(int j = 0 ; j < size - 1; j++)
    	{
    	String edge_added = find_min_distance_and_update_connected_components(sets, size);
    	System.out.println(j + "  = edge added = " + edge_added);
    	}
    	
    	System.out.println("Sanity Check");
    	
     	for(int j = 0 ; j < size; j++)
    	{ 
     		
    	   System.out.println("d = " + sets[j][0]);
    	}
    	
     	return null;
    }
    
    static String printMST(double[] parent, double[][] graph, Integer V, Integer[] map)
    {
        System.out.println("Edge   Weight");
        String s = "";
        for (Integer i = 1; i < V; i++){
          
        	
        	System.out.println(parent[i]+ " - "+ i+ " " +
                               graph[i][(int) parent[i]]);
        	s = s + ((int)parent[i]) + " " + i.toString() + ";";
  
        }
        
        System.out.println(s);
        
        return s;
    }
    
    
    static String Prim(String[] points){
    	
    	Integer n = points.length;
    	Double length = 0.0;
    	
    	if(n == 0){
    		return null;
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
            System.out.println("bestdistance added = " + bestdist);
            System.out.println("edge_added = " + edge_added);
            System.out.println("bestdist = " + bestdist);
            done[best] = true;
            for (int j = 0; j < n; j++) {
              if (!done[j]&& dist[j] > compute_distance(points[best], points[j])) {
            	  System.out.println(points[best]  + " ," + points[j]);
                dist[j] = compute_distance(points[best], points[j]);
                vertex[j] = points[best]; 
                System.out.println("vertex[j] = " + vertex[j] );
                System.out.println("distance = " + dist[j]);
                
                
              }
            }
          }
    	
    	System.out.println("length = " + length);
    	System.out.println("primMST = " + prim_mst);
    	return prim_mst;
    }
    
  
    
    public static String PrimShort(String[] points, double maxLength){
        System.out.println("Entering PrimShort...");
           
         
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
              System.out.println("length added = " + bestdist);
            }      
             
            //System.out.println("Exiting loop...");
        }
       // System.out.println("Exiting PrimShort...");
         
     //   for (Point p : points) {
     //     System.out.println(p.print());
      //  }
        
        for(int i = 0; i < n; i++){
        	if(done[i] == false){
        		short_mst1 = short_mst1 + points[i] + "," + points[i] + ";C";
        	}
        }
        System.out.println("short mst = " + short_mst);
        System.out.println("short mst with connected components = " + short_mst1);
        System.out.println("lenght = " + length);
        return short_mst1;
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
  	  System.out.println("n = " + n);
  	  
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
//  	  System.out.println("Exiting epsNet...");
  	  
  	  for(String ress : res){
  		  //System.out.println("res = " + ress);
  	  }
  	  
  	  String[] myArray = res.toArray(new String[res.size()]);
  	  return myArray;
    }

    
    // Function to construct and print MST for a graph represented
    //  using adjacency matrix representation
    static String primMST(double[][] graph, Integer V, Integer[] map)
    {
        // Array to store constructed MST
        double parent[] = new double[V];
 
        // Key values used to pick minimum weight edge in cut
        double key[] = new double [V];
 
        // To represent set of vertices not yet included in MST
        Boolean mstSet[] = new Boolean[V];
 
        // Initialize all keys as INFINITE
        for (int i = 0; i < V; i++)
        {
            key[i] = Integer.MAX_VALUE;
            mstSet[i] = false;
        }
 
        // Always include first 1st vertex in MST.
        key[0] = 0;     // Make key 0 so that this vertex is
                        // picked as first vertex
        parent[0] = -1; // First node is always root of MST
 
        // The MST will have V vertices
        for (int count = 0; count < V-1; count++)
        {
            // Pick the minimum key vertex from the set of vertices
            // not yet included in MST
            int u = minKey(key, mstSet, V);
 
            // Add the picked vertex to the MST Set
            mstSet[u] = true;
 
            // Update key value and parent index of the adjacent
            // vertices of the picked vertex. Consider only those
            // vertices which are not yet included in MST
            for (int v = 0; v < V; v++)
 
                // graph[u][v] is non zero only for adjacent vertices of m
                // mstSet[v] is false for vertices not yet included in MST
                // Update the key only if graph[u][v] is smaller than key[v]
                if (graph[u][v]!=0 && mstSet[v] == false &&
                    graph[u][v] <  key[v])
                {
                    parent[v]  = u;
                    key[v] = graph[u][v];
                }
        }
 
        // print the constructed MST
        String s = printMST(parent, graph, V, map);
        
        System.out.println("value returned by PrimMST = " + s);
        
        return s;
    }

    
	static void print_graph(Integer[][] graph, int V){
		
		for(int i = 0; i < V; i++){
			for(int j = 0; j < V; j++){
				System.out.println("Graph[" + i + "][" + j + "] = " + graph[i][j]);
			}
		}		
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
	
	

	static double[][] get_graph(Integer V) throws IOException{
	   	    
           FileReader fr = new FileReader("/home/avadapal/workspace/myFirstSpark/src/MST/input.txt");
           BufferedReader reader = new BufferedReader(fr);
           
           System.out.println("Reading File line by line using BufferedReader");
         
           String line = reader.readLine();
           System.out.println("Reading File line by line using BufferedReader");
           System.out.println("V = " + V);
           double graph[][] = new double[V][V];
          // graph[16][16] = 0;
           String[] lines = new String[V];
        int k = 0;
		    while (line != null) {
		        //System.out.println("k = " + k);
		       lines[k] = line;
		     line = reader.readLine();
		     k++;
		    }
		    reader.close();
		    System.out.println("Reading File line by line using BufferedReader" + V);
				for(int i = 0; i < V; i++){ 
					//System.out.println("V = " + V);
					String token1[] = lines[i].split(";");
					for(int j = 1; j < V; j++){
					//System.out.println("j = " + j);	
					String token2[] = lines[j].split(";");
					//System.out.println("V = " + V);
					double d =  compute_distance(token1[1], token2[1]);
					//System.out.println("token1 = " + token1[1] + " --- " + token1[0]);
					//System.out.println("token2 = " + token2[1] + " --- " + token2[0]);
					//System.out.println("i = " + i + " and j = " + j);
					graph[i][j] = d; 
					graph[j][i] = d;					
					}
				}			
		System.out.println("Done");
		return graph;
	}
	
	public static void main(String[] args) throws IOException{
	
		
		
		FileWriter fw = new FileWriter("/home/avadapal/workspace/myFirstSpark/src/MST/input1.txt");
		
		Random rand = new Random();
		
		for(int i = 1; i <= 20000; i++)
		{
			int  n1 = rand.nextInt(50) + 1;
			int  n2 = rand.nextInt(50) + 1;
			int  n3 = rand.nextInt(50) + 1;
			String s = i +";"+ n1 + " " + n2; // + " " + n3;
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
	        		//System.out.println("max = " + max);
	        	}
	         }
		    }
	        else{
	        	//System.out.println("t = " + t);
	        	break;
	        }
	        
			nlines1++;
			
		}
		
		reader.close();
		final int nlines = nlines1;
		final int Delta = max;
		System.out.println("Delta = " + Delta);
		
		System.out.println("Compute Minimum Spanning Tree");
	    System.out.println("lines = " + nlines);
	    
	    //double[][] graph = get_graph(nlines);

	    //SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("spark://avadapal-XPS-13-9350:7077").set("spark.executor.memory","2g").set("spark.cores.max", "4");
	    SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("local[4]").set("spark.executor.memory","1g");		
	    //SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("spark://localhost:7077").set("spark.executor.memory","2g");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);	    
		System.out.println("Collection being done!");

		JavaRDD<String> lines = ctx.textFile(inputPath, 16); 
	    
		//TODO: Change this style of Heirarchical Clustering.		
		
		System.out.println("Before Clustering");
		
		JavaPairRDD<Integer, String> heirarchical_clusters = lines.mapToPair(new PairFunction<String, Integer, String>()
				{
			  Integer Key = 0;
              public Tuple2<Integer, String> call(String s){
         	   String[] tokens  = s.split(";");
         	  System.out.println(nlines);
         	  String[] coordinates = tokens[1].split(" ");
         	  
         	  Integer heirarchichal_break = Delta/2 + 1;
         	  System.out.println("hbreak  = " + heirarchichal_break);
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
         	  System.out.println("bin = " + bin);
         	  Integer key = Integer.parseInt(bin, 2) + 1;
         	     	   
         	   return new Tuple2<Integer, String>(key, s);
          	   //return new Tuple2<Integer, String>(Integer.parseInt(tokens[0]), (tokens[1]));
            }
			}
	
	); 
		

	System.out.println("Clustering Done");	
		
	System.out.println(heirarchical_clusters.collect());
	System.out.println("Heirarchical Clusters Above ");		
	JavaPairRDD<Integer, Iterable<String>> h_grp = heirarchical_clusters.groupByKey();	
	System.out.println(h_grp.collect());	
	System.out.println("Grouped Values Above");
	
	
	//Local Computations Here.
	
	JavaPairRDD<Integer, String> h_grp1 = h_grp.mapValues(new Function<Iterable<String>, String>() {
	public String call(Iterable<String> s) {
    	
         Integer no_of_ver =0;
		 Iterator<String> s1 = s.iterator();
         while (s1.hasNext()) {
		  s1.next();
		  no_of_ver++;
		}
         
         
		
        System.out.println("Number of Vertices = " + no_of_ver);

		String l = null;
		double[][] graph = new double[no_of_ver][no_of_ver];
		Integer[] map = new Integer[no_of_ver];
		String[] cords = new String[no_of_ver];
		int i = 0;
	    for(String t: s){
	    	
	    	String tokens[] = t.split(";");
	    	map[i] = Integer.parseInt(tokens[0]);
	    	cords[i] = tokens[1];
	    	i++;
	    	System.out.println(t);
	    }
	    
	    for(String cor: cords){
	    	System.out.println("co = " + cor);
	    }
	    
	   for(int j = 0; j < i ; j++ ){
	    	for(int k = 0; k < i; k++){
	    		graph[j][k] = compute_distance(cords[j], cords[k]);
	    	}
	    } 
 	   
    l = primMST(graph, no_of_ver, map);
	String tks[] = l.split(";");
	String localMST = "";
	for(String tt : tks){
		System.out.println("tt = " + tt);
		String tks1[] = tt.split(" ");
		for(String ttt : tks1){
			String cordinate = cords[Integer.parseInt(ttt)];
			System.out.println("cordinate = " + cordinate);
			localMST = localMST + cordinate + ",";
		}
		
		
		localMST = localMST + ";";
	}

	String new_prim = Prim(cords);
	
	 ArrayList<String> cords1 = new ArrayList<String>(Arrays.asList(cords));
	 String[] net = epsNet(cords1, 0.8);
	
	 System.out.println("Printing the Net: ");
	 for(String nn : net){
		 System.out.println("net = " + nn);
	 }

	String connected_comps = PrimShort(net, 7.8);
	
	System.out.println("connected_comps = " + connected_comps);
	
	
	System.out.println("new_prim = " + new_prim);
	System.out.println("localMST = " + localMST);
    System.out.println("MST computed Here");    	
	
    return connected_comps;
    //return new_prim;
    //return localMST;
	}
	});
	
	System.out.println(h_grp1.collect());
	System.out.println("h_grp1 above");
	
	JavaRDD<String> values = h_grp1.values();	
	List<String> vals = values.collect();
	List<String> vals1 = new ArrayList<String>(); 
	for(String ss : vals){
		System.out.println("ss = " + ss);
		String[] tokens = ss.split("C");
		for(String sss: tokens){
			System.out.println("sss = " + sss);
			if(sss.length() > 0 && (sss != null || sss !="" || !sss.isEmpty()))	vals1.add(sss);	
		}
		
	}
	
	
	System.out.println("Printing Vals1");
	
	for(String tt: vals1){
		System.out.println("tt = " + tt);
	}
	
	
	combine_connected_components(vals1);
	System.out.println("End of the Program");
	
	System.out.println("a debug");
	
	String[] Input = {"1 4", "4 4", "2 2", "1 1"};
	
	String[] Inp = {"1 1", "1 2", "1 3", "5 1", "5 2", "5 3", "9 1", "9 2", "9 3", "14 1", "14 2", "14 3", "1 4", "1 5", "1 6", "14 4", "14 5"  };
	ArrayList<String> Inp1 = new ArrayList<String>(Arrays.asList(Input));
	//ArrayList<String> Inp1 = Input; 
	
	String short_prim = PrimShort(Inp, 1.2);
	//String regularmst= Prim(Input);
	System.out.println("short prim = " + short_prim);
    //System.out.println("regulat mst = " + regularmst);
	//String[] net = epsNet(Inp1, 3);
	//System.out.println("Printing the net ...");
//	for(String ne : net){
	///	System.out.println("ne = " + ne);
//	}

	}

}
