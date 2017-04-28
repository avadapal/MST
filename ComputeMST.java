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
    static int minKey(int key[], Boolean mstSet[], Integer V)
    {
        // Initialize min value
        int min = Integer.MAX_VALUE, min_index=-1;
 
        for (int v = 0; v < V; v++)
            if (mstSet[v] == false && key[v] < min)
            {
                min = key[v];
                min_index = v;
            }
 
        return min_index;
    }
	
    
    
    static String rNet(double r, String set){
    	
    	String cords[] = set.split(" ");
    	
    	
    	return null;
    }
    
    static String compute_min_dist_between_two_sets(String[] set1, String[] set2){
    	
    	Integer Min = Integer.MAX_VALUE;
    	String new_edge = "";
    	for(String s1 : set1){
    		if(s1 == set1[0]) continue;
    		String cords1[] = s1.split(",");
     		for(String s2: set2){
     			String cords2[] = s2.split(",");
    			if(s2 == set2[0]) continue;
    			Integer dist1 = compute_distance(cords1[0], cords2[1]);
    			if(dist1 < Min){
    				Min = dist1;
    				new_edge = cords1[0] + "," + cords2[1];
    			}
    			Integer dist2 = compute_distance(cords1[0], cords2[0]);
    			if(dist2 < Min){
    				Min = dist2;
    				new_edge = cords1[0] + "," + cords2[0];
    			}
    			Integer dist3 = compute_distance(cords1[1], cords2[0]);
    			if(dist3 < Min){
    				Min = dist3;
    				new_edge = cords1[1] + "," + cords2[0];
    			}
    			Integer dist4 = compute_distance(cords1[1], cords2[1]);
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
    	Integer min = Integer.MAX_VALUE;
    	String edge_added = "";
    	Integer c1 = 0;
    	Integer c2 = 0;
    	
    	for(int k = 0; k < size ; k++){	
        	for(int j = k; j < size; j++){
        		//System.out.println(sets[k][0] + "<->" + sets[j][0]);
        		if(sets[k][0] != sets[j][0]){
        		String res = compute_min_dist_between_two_sets(sets[k], sets[j]);
        		String tok[] = res.split("_");
        		String new_edge = tok[0];
        		Integer cur_min = Integer.parseInt(tok[1]);
        		if(cur_min < min){
        			min = cur_min;
        			edge_added = new_edge; 
        			c1 = j;
        			c2 = k;
        		}
        		
        		}
        	}       	
        }
    	
    	if(c1 < c2){
    		sets[c2][0] = sets[c1][0];
    	}
    	else{
    		sets[c1][0] = sets[c2][0];
    	}
    
    	//System.out.println("c1 = " + c1 + " , " + "c2 = " + c2);
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
    		//ss = connected_comp + ";" + ss;
    		String toks[] = ss.split(";");
    		sets[i] = toks;
    		i++;
    		connected_comp++;
    	}
    	
    	System.out.println("Some Debugging");
    
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
    
    static String printMST(Integer[] parent, Integer[][] graph, Integer V, Integer[] map)
    {
        System.out.println("Edge   Weight");
        String s = "";
        for (Integer i = 1; i < V; i++){
          
        	
        	System.out.println(parent[i]+ " - "+ i+ " " +
                               graph[i][parent[i]]);
        	s = s + (parent[i].toString()) + " " + i.toString() + ";";
  
        }
        
        System.out.println(s);
        
        return s;
    }
    
    // Function to construct and print MST for a graph represented
    //  using adjacency matrix representation
    static String primMST(Integer[][] graph, Integer V, Integer[] map)
    {
        // Array to store constructed MST
        Integer parent[] = new Integer[V];
 
        // Key values used to pick minimum weight edge in cut
        int key[] = new int [V];
 
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
	static Integer compute_distance(String x, String y){
		
		String[] x1 = x.split(" ");
		String[] x2 = y.split(" ");
		
		Integer distance  = 0;
		for(Integer i = 0; i < x1.length; i++){
		 	
			Integer x1_i = Integer.parseInt(x1[i]);
			Integer x2_i = Integer.parseInt(x2[i]);
			
			distance += (x1_i - x2_i) * (x1_i - x2_i);
		}
		
		return distance;
	}
	
	

	static Integer[][] get_graph(Integer V) throws IOException{
	   	    
           FileReader fr = new FileReader("/home/avadapal/workspace/myFirstSpark/src/MST/input.txt");
           BufferedReader reader = new BufferedReader(fr);
           
           System.out.println("Reading File line by line using BufferedReader");
         
           String line = reader.readLine();
           System.out.println("Reading File line by line using BufferedReader");
           System.out.println("V = " + V);
           Integer graph[][] = new Integer[V][V];
           graph[16][16] = 0;
           String[] lines = new String[V];
        int k = 0;
		    while (line != null) {
		        System.out.println("k = " + k);
		       lines[k] = line;
		     line = reader.readLine();
		     k++;
		    }
		    reader.close();
		    System.out.println("Reading File line by line using BufferedReader" + V);
				for(int i = 0; i < V; i++){ 
					System.out.println("V = " + V);
					String token1[] = lines[i].split(";");
					for(int j = 1; j < V; j++){
					System.out.println("j = " + j);	
					String token2[] = lines[j].split(";");
					System.out.println("V = " + V);
					Integer d =  compute_distance(token1[1], token2[1]);
					System.out.println("token1 = " + token1[1] + " --- " + token1[0]);
					System.out.println("token2 = " + token2[1] + " --- " + token2[0]);
					System.out.println("i = " + i + " and j = " + j);
					graph[i][j] = d; 
					graph[j][i] = d;					
					}
				}			
		System.out.println("Done");
		return graph;
	}
	
	public static void main(String[] args) throws IOException{
		
		
		
	
		
		BufferedReader reader = new BufferedReader(new FileReader("/home/avadapal/workspace/myFirstSpark/src/MST/input.txt"));
		int nlines1 = 0;
		int max = 0;
		while (true){
			
	       String t = reader.readLine();
	       
	       System.out.println("t = " + t);
	        if(t != null)
	        {    
	        String linesplit[] = t.split(";");
	       
	        String cordsplit[] = linesplit[1].split(" ");
	        for(String a: cordsplit){
	        	if(Integer.parseInt(a) > max){
	        		max = Integer.parseInt(a);
	        		System.out.println("max = " + max);
	        	}
	         }
		    }
	        else{
	        	System.out.println("t = " + t);
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
	    
	    Integer[][] graph = get_graph(nlines);
	    
	    SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("local");		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);	    
		System.out.println("Collection being done!");
		FileWriter fw = new FileWriter("/home/avadapal/workspace/myFirstSpark/src/MST/input1.txt");
		
		Random rand = new Random();
		
		for(int i = 1; i <= 1000; i++)
		{
			int  n1 = rand.nextInt(50) + 1;
			int  n2 = rand.nextInt(50) + 1;
			int  n3 = rand.nextInt(50) + 1;
			String s = i +";"+ n1 + " " + n2 + " " + n3;
			//System.out.println("s = " + s);
			fw.write(s + '\n');
             		
		}
        fw.close();
		JavaRDD<String> lines = ctx.textFile("/home/avadapal/workspace/myFirstSpark/src/MST/input.txt", 1); 
	    
		//TODO: Change this style of Heirarchical Clustering.		
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
		Integer[][] graph = new Integer[no_of_ver][no_of_ver];
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

	System.out.println("localMST = " + localMST);
    System.out.println("MST computed Here");    	
	return localMST;
	}
	});
	
	System.out.println(h_grp1.collect());
	System.out.println("h_grp1 above");
	
	JavaRDD<String> values = h_grp1.values();	
	List<String> vals = values.collect();
	combine_connected_components(vals);
	System.out.println("End of the Program");

	}

}
