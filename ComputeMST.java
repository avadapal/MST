//package MST;

import scala.Tuple2;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction; 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Arrays;
import java.util.Collections;

import java.util.HashSet;
import java.util.List; 
import java.util.ArrayList; 
import java.util.Random;
import java.util.Set;


import org.apache.spark.api.java.function.Function; 
//import java.util.Map; 
import java.util.Iterator; 

import org.apache.log4j.*;

import org.apache.commons.math3.util.Pair;

public class ComputeMST implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	static utilities ut;

   	
	public static epsNet union_Eps_Nets(epsNet net1, epsNet net2){

		epsNet union =  new epsNet();
		
		union.length_MSF = net1.length_MSF + net2.length_MSF;
		ArrayList<Point> net1_nodes = net1.nodes_MSF;
		ArrayList<Point> net2_nodes = net2.nodes_MSF;
		
		List<TreeEdge> net1_edges = net1.edges;
		List<TreeEdge> net2_edges = net2.edges;
		net1_edges.addAll(net2_edges);
		
		net1_nodes.addAll(net2_nodes);
		union.nodes_MSF = net1_nodes;
		union.edges = net1_edges;
		

		return union;
	}
	
	
	public static double reevaluate_connected_comp(List<epsNet> epsNetslogn){
		

	double total_len = 0;

	for(epsNet net: epsNetslogn){
		total_len += net.length_MSF;
	} 	
	return total_len;
  }
	
    
    static double Prim(String[] points){
    	
    	Integer n = points.length;
    	Double length = 0.0;
    	int number_edges = 0;
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
            number_edges += 1;
    
            prim_mst = prim_mst + edge_added + ";";
            done[best] = true;
            for (int j = 0; j < n; j++) {
              if (!done[j]&& dist[j] > utilities.compute_distance(points[best], points[j])) {
                dist[j] = utilities.compute_distance(points[best], points[j]);
                vertex[j] = points[best]; 
              }
            }
          }
    	
    	return length;
    }
    
  
    
    public static EpsilonMST PrimShortNew(epsNet Net, double maxLength){
    
    	
      ArrayList<Point> points	= Net.nodes_MSF; 

      Integer[] map = new Integer[points.size()];	
  	  int current_connected_component = Integer.MAX_VALUE;
	  

	  current_connected_component = points.get(0).connected_component;

  	  
  	  EpsilonMST mst = new EpsilonMST();
  	  List<Pair<Point, Point>> epsMSTedges = new ArrayList<Pair<Point, Point>>();
	  List<TreeEdge> treeedges = new ArrayList<TreeEdge>();
        
        double length = 0.0;
        int n = points.size();
        boolean[] done = new boolean[n]; 
        
        if (n == 0) {
            System.out.println("Terminating Prim since n = 0.");
            return mst;
        }
     
        HashSet<Integer> conn = new HashSet<Integer>();
        Double[] dist = new Double[n];
        Point[] vertex = new Point[n];

        Integer[] color = new Integer[n];
        
        for(int itt = 0 ; itt < n; itt++){
        	color[itt] = points.get(itt).connected_component;
        	done[itt] = false;
        }
        
        for (int it = 0; it < n; it++) {
      	  
      	  Integer curcolor = color[it];
      	  Integer curComp = points.get(it).connected_component;
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
          
            while (true) {  
          	  long oldColor = color[best];
                Point cPoint = null;
                for (int i = 0; i < n; i++) {
                Point curPoint = points.get(i);
                
                
                
                if (color[i] == oldColor) {
                      color[i] = curcolor;
                      points.get(i).connected_component = curComp;
                      for (int j = 0; j < n; j++) {
                      	if (dist[j] > distance(curPoint, points.get(j))) { 
                              dist[j] = distance(curPoint, points.get(j));                          
                              vertex[j] = curPoint;                         
                              map[j] = i;
                              cPoint = curPoint;
                       
                          }  
                      }
                    
                  }          
                }
                 
              best = -1;
              double bestdist = Double.MAX_VALUE;
              int node = 0;
              for (int i = 0; i < n; i++) {
                  if (curcolor != color[i] && dist[i] < bestdist && dist[i] <= maxLength) {
                      best = i;
                      bestdist = dist[i];
                      node = i;
                      if(bestdist != 0.0){
                      
                      }
                  }
              }
              
              if (best == -1) {                	
                  break;
              }
              
              length += bestdist;

              epsMSTedges.add( new Pair<Point, Point> (points.get(node), points.get(map[node])) );
              TreeEdge edge = new TreeEdge();
              edge.left = points.get(node);
              edge.right = points.get(map[node]);
              treeedges.add(edge);
              
              done[map[node]] = true;
              done[node] =  true;
            }      
   
        }
        
        for(int i = 0; i < n; i++){
        	if(done[i] == false){
        		mst.singletons.add(points.get(i));
        	}
        }

        treeedges.addAll(Net.edges);
        mst.edges = epsMSTedges;
        mst.tree_edges = treeedges;
        mst.length = length + Net.length_MSF;

        mst.number_of_connected_components = current_connected_component;
  	
      
        return mst;
      } 
    
    
    public static EpsilonMST PrimShortNew(ArrayList<Point> points, double maxLength){

    	Integer[] map = new Integer[points.size()];	
      	  int current_connected_component = Integer.MAX_VALUE;
          ArrayList<Integer> connected_indices = new ArrayList<Integer>(); 	
      	  
          for(Point p: points){
      		
        	  if(p.connected_component < current_connected_component){
      			current_connected_component = p.connected_component;
      		  }
      		  connected_indices.add(p.connected_component);
      	  }
    	  
      	  Set<Integer> connectedUniqueValues = new HashSet<Integer>(connected_indices);
      	  
      	  Integer[] connected_unique = connectedUniqueValues.toArray(new Integer[connectedUniqueValues.size()]);
      	  int current_connected_index = 0;
      	  current_connected_component = connected_unique[current_connected_index];
      	
      	  current_connected_component = points.get(0).connected_component;
    	
    	  
    	  EpsilonMST mst = new EpsilonMST();
    	  List<Pair<Point, Point>> epsMSTedges = new ArrayList<Pair<Point, Point>>();
    	  List<TreeEdge> treeedges = new ArrayList<TreeEdge>();
          
          double length = 0;
          int n = points.size();
          boolean[] done = new boolean[n]; 
          
          if (n == 0) {
              System.out.println("Terminating Prim since n = 0.");
              return mst;
          }
       
          HashSet<Integer> conn = new HashSet<Integer>();
          Double[] dist = new Double[n];
          Point[] vertex = new Point[n];

          Integer[] color = new Integer[n];
          
          for(int itt = 0 ; itt < n; itt++){
          	color[itt] = points.get(itt).connected_component;
            
          	done[itt] = false;
          }
        	 
    	  for (int it = 0; it < n; it++) {
        	  Integer curcolor = color[it];
        	  Integer curComp = points.get(it).connected_component;
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
            
              int itt = 0;
              while (true) {
            	  
            	  itt++;
            	  long start = System.nanoTime();
            	  long oldColor = color[best];
                  int node2 = 0;
                  for (int i = 0; i < n; i++) {
                  Point curPoint = points.get(i);
                  if (color[i] == oldColor) {
                        color[i] = curcolor;
                        points.get(i).connected_component = curComp;
                        for (int j = 0; j < n; j++) {
                        	if (dist[j] > distance(curPoint, points.get(j))) { 
                                dist[j] = distance(curPoint, points.get(j));
                                
                                
                                vertex[j] = curPoint; // points[best];
                                node2 = i;
                                map[j] = i;
                            }  
                        }
                      
                    }          
                  }
                   
                best = -1;
                double bestdist = Double.MAX_VALUE;
                int node = 0;
                for (int i = 0; i < n; i++) {
                    if (curcolor != color[i] && dist[i] < bestdist && dist[i] <= maxLength) {
                        best = i;
                        bestdist = dist[i];
                        node = i;
                        if(bestdist != 0.0){
                        
                        }
                    }
                }
                
                
                if (best == -1) {                	
                    break;
                }
                
                length += bestdist;
                
                TreeEdge edge = new TreeEdge();
                epsMSTedges.add( new Pair<Point, Point> (points.get(node), points.get(node2)) );
                edge.left = points.get(node);
                edge.right = points.get(map[node]);
                treeedges.add(edge);
                          
                done[map[node]] = true;
                done[node] =  true;
     
              
              }      
     
     
          }
    
          for(int i = 0; i < n; i++){
          	if(done[i] == false){
          		mst.singletons.add(points.get(i));
          		
          	}
          }
          

          
          mst.tree_edges = treeedges;

          
          mst.edges = epsMSTedges;
          mst.length = length;
          mst.number_of_connected_components = current_connected_component;

          return mst;
        } 
     

 public static ArrayList<Point>  epsNetNew(ArrayList<Point> points, double distance) {

	 int n = points.size();
	  double [] dist = new double[n];
	  for (int i = 0; i < n; i++) {
		  dist[i] = Double.MAX_VALUE;
	  }
	  ArrayList<Point> result = new ArrayList<Point>();
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
		  result.add(points.get(furthest));
		  for (int i = 0; i < n; i++) {
			  dist[i] = Math.min(dist[i], distance(points.get(furthest), points.get(i)));
		  }
	  }

	  return result;
 }

    
	
	public static double seq_MST(String input) throws IOException{
		
		BufferedReader reader = new BufferedReader(new FileReader(input));
		
		int nlines1 = 0;
	
		while (true){
			
	       String t = reader.readLine();

	        if(t == null){	        	
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
	
	
	static double distance(Point p1, Point p2){
		
		double distance = 0.0;
		int dimension = p1.dimension;
		for(int d = 0; d < dimension; d++){
			distance += (p1.coordinates[d] - p2.coordinates[d]) * (p1.coordinates[d] - p2.coordinates[d]);
			
		}
		
		
		distance = Math.sqrt(distance);
		return distance;
	}
	
		
public static void main(String[] args) throws IOException{
		
		
	Logger.getLogger("org").setLevel(Level.OFF); 		
	Logger.getLogger("akka").setLevel(Level.OFF);
			
 
	SparkConf sparkConf = new SparkConf().setAppName("ComputeMST").setMaster("local[200]").set("spark.executor.memory = 10g", "spark.cores.max = 200");		 
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);	          
  


 
  List<TreeEdge> edges = new ArrayList<TreeEdge>();
 
 // This is running the MST algorithm log(n) times
  ArrayList<Point> all_pointslogn = new ArrayList<Point>();
  List<TreeEdge> Final_Tree_Edges = new ArrayList<TreeEdge>();
  
  
  
 for(int ii = 0; ii < 1; ii++) //Main Loop Begins ...
 {
	
	   all_pointslogn.clear();

	   System.out.println("length of arguements = " + args.length);
	 
       if(args.length > 1){
	    	System.out.println("arguement read");
	    }
	  
	    String inputPath = args[0];
		BufferedReader reader = new BufferedReader(new FileReader(inputPath));
		
		int nlines1 = 0;
		
		double max = 0;
		int dim = 0;
		
		while (true){
			
	       String t = reader.readLine();

	        if(t != null){    
	        String linesplit[] = t.split(";");
	       
	        String cordsplit[] = linesplit[1].split(" ");
	        int d1 = 0;
	        for(String a: cordsplit){
	        	if(Double.parseDouble(a) > max){
	        		max = Double.parseDouble(a);
	        	}
	        	d1 ++;
	         }
	         dim = d1;
		    }
	        else{
	        	break;
	        }
	        
			nlines1++;			
		}
		
		reader.close();
		System.out.println("dim = " + dim);

		final int nlines = nlines1;
		final double Delta = max + 1;
		final double eps = Double.parseDouble(args[2]);
		final int d = dim;
		final int number_of_levels = Integer.parseInt(args[3]);
		
		final double[] rand_v = new double[d];

		//The vector for the random shift
		for(int j = 0; j < d; j++){
					rand_v[j] = Math.random();
				}

		
		
		System.out.println("Delta = " + Delta);
		long startTime_dis = System.nanoTime(); 
		System.out.println("Compute Minimum Spanning Tree");
	    System.out.println("lines = " + nlines);	
	    
	    System.out.println("Collection being done!");
	    
		JavaRDD<String> lines = ctx.textFile(args[1], 16);
		
		
		
		//lines.collect();
    
		System.out.println("Lines collected");
		
		class unit_step implements
		Function<Iterable<Point>, epsNet> {
			
			private static final long serialVersionUID = 1L;
			int div = 1;
			
			//Constructor
			public unit_step(int den) {
				div = den;
			}
			
			public epsNet call( Iterable<Point> p) {
				 double Delta_l   = Delta * 2.0 * Math.sqrt(d) * 0.035; 
				 double eps2Delta = eps * eps * (Delta_l/div);
				 double epsDelta  = eps * (Delta_l/div);
						
				 Integer no_of_ver = 0;
				 Iterator<Point> s1 = p.iterator();
		         while (s1.hasNext()) {
				  s1.next();
				  no_of_ver++;
				}
		    
		    double[][] coordinates = new double[no_of_ver][];		    
		    ArrayList<Point> points = new ArrayList<Point>();    
		    
		    int i = 0;		    
		    points.clear();
		    
		    for(Point point: p){
		    	coordinates[i] = point.coordinates;
		    	points.add(point);
		    	i++;
		    }

			EpsilonMST epsMST = PrimShortNew(points, epsDelta);
	
			ArrayList<Point> epsNetMSF = epsNetNew(points, eps2Delta); 

			epsNet net = new epsNet();
			
			net.length_MSF = epsMST.length;
			net.nodes_MSF = epsNetMSF;	
			net.edges = epsMST.tree_edges;

			return  net;
	 
			}
			};
		
		
		class next_level implements
		PairFunction<Tuple2<ArrayList<Integer>, epsNet>,ArrayList<Integer> , epsNet> {
		private static final long serialVersionUID = 1L;
				@Override
		public Tuple2<ArrayList<Integer>, epsNet> call(
		Tuple2<ArrayList<Integer>, epsNet> pair) throws Exception {					 
			
					ArrayList<Integer> new_key = (pair._1);
					new_key.remove(new_key.size() - 1);
			
					return new Tuple2<ArrayList<Integer>, epsNet>(new_key, pair._2);	
				}
				}
				
		
				class union_eps_nets implements
				Function2<epsNet, epsNet, epsNet > {
				
					private static final long serialVersionUID = 1L;  								
				public epsNet call(epsNet n1, epsNet n2) {
			
				return union_Eps_Nets(n1, n2);
				}
				}
				
				
				class unit_step_logn implements
				Function<epsNet, epsNet> {
					int den = 0;
					int current_level = 0;
					private static final long serialVersionUID = 1L;
					public unit_step_logn(int div_by, int lev) {
		
						den = div_by;
						current_level = lev;
						// TODO Auto-generated constructor stub
					}
					public epsNet call(epsNet epsnet) {
						 
						 Iterable<Point> p = epsnet.nodes_MSF;
						 double Delta_l = Delta * 2.0 * Math.sqrt(d) * Math.pow(10, (current_level - number_of_levels - 1)/d);
						 double eps2Delta = eps * eps * (Delta_l/den);
						 double epsDelta = eps * (Delta_l/den);
						
						 Integer no_of_ver = 0;
						 Iterator<Point> s1 = p.iterator();
				         while (s1.hasNext()) {
						  s1.next();
						  no_of_ver++;
						}
				         
				    double[][] coordinates = new double[no_of_ver][];
				    
				    int i = 0;
				    ArrayList<Point> points = new ArrayList<Point>();
				    points.clear();
				    for(Point point: p){
				    	coordinates[i] = point.coordinates;
				    	points.add(point);
				    	i++;
				    }
				  
					EpsilonMST epsMST = PrimShortNew(epsnet, epsDelta);	
					
					ArrayList<Point> epsNetMSF = epsNetNew(points, eps2Delta); 					
					epsNet net = new epsNet();					
					net.length_MSF = epsMST.length;
					net.nodes_MSF = epsNetMSF;
					net.edges = epsMST.tree_edges;
					return  net;

					}
					};
						
		JavaRDD<Point> PointRDD = lines.map(new Function<String, Point>() {
	
		private static final long serialVersionUID = 1L;

		public Point call(String x) { 
		
			String tokens[] = x.split(";");
			int id = Integer.parseInt(tokens[0]);
				
			String[] cord_string = tokens[1].split(" ");
			double[] coords = new double[d];
		
	
			for(int j = 0; j < d; j++){
 					coords[j] = Double.parseDouble(cord_string[j]);
 					coords[j] += rand_v[j];			
 				}
				
 			Point p = new Point(d, coords, 0, id);		
			return p; 
			}
			
		});
		
		
	//PointRDD.collect();
	
	JavaPairRDD<ArrayList<Integer>, Point> h_clusters_logn = PointRDD.mapToPair(new PairFunction<Point, ArrayList<Integer>, Point>(){
		
	private static final long serialVersionUID = 1L;
	public Tuple2< ArrayList<Integer>, Point> call(Point p){
	
		ArrayList<Integer> levels = new ArrayList<Integer>();
		double[] cordinates = p.coordinates;
		String cluster_binary = "";
		int n_levels = number_of_levels;
		String[] clusters = new String[n_levels];
		
		for(int j = 0; j < n_levels; j++ ){
				clusters[j] = "";
			}
			
		int count = 0;
		for(double x_i: cordinates){
			double rand_i =  rand_v[count];
			count++;
			double left_corner = 0.0;
			double size = Delta;
			//System.out.println("size = " + size);
			for(int i = 0; i < n_levels; i++){
					double comparitor = size/2 + left_corner;
			        if(x_i - rand_i > comparitor){
       		          cluster_binary = cluster_binary + "1";
       		          clusters[i] = clusters[i] + "1";
       		          left_corner += size/2;
 		         	}
       		      else{
        			cluster_binary = cluster_binary + "0";
        			clusters[i] = clusters[i] + "0";
       		     }
			        size = size/2;
			}
				
		 }		
			for(int i = 0; i < n_levels; i++){
				String bin =  clusters[i];
				Integer key = Integer.parseInt(bin, 2) + 1;
				levels.add(key);
			}

		   return new Tuple2<ArrayList<Integer> , Point> (levels, p);
		}
	});	
	

	
	
	JavaPairRDD<ArrayList<Integer> ,Iterable<Point>> h_clusters_logn_grouping = h_clusters_logn.groupByKey();	

	int level_counter = 0;
	
	int den = (int) Math.pow(2, number_of_levels);

	JavaPairRDD<ArrayList<Integer>, epsNet> distributed_msts_logn = h_clusters_logn_grouping.mapValues(new unit_step(den));

	while(true){

	level_counter++;

	if(level_counter > (number_of_levels - 1)) break;
	
	System.out.println("LEVEL = " + level_counter);
	
	JavaPairRDD<ArrayList<Integer>, epsNet> distributed_msts_logn1 = distributed_msts_logn.mapToPair(new next_level());
	
	JavaPairRDD<ArrayList<Integer>, epsNet> distributed_msts_next_level = distributed_msts_logn1.reduceByKey(new union_eps_nets());

	den = den/2;
	
	distributed_msts_logn = distributed_msts_next_level.mapValues(new unit_step_logn(den, level_counter));		
	}

	JavaRDD<epsNet> epsNetsRDDlogn =  distributed_msts_logn.values();

	List<epsNet> epsNetslogn = epsNetsRDDlogn.collect(); 	

	double lenlogn = reevaluate_connected_comp(epsNetslogn); 
	
	Final_Tree_Edges.clear();

	for(epsNet net: epsNetslogn){
		Final_Tree_Edges.addAll(net.edges);
	}   
	
	for(epsNet nets: epsNetslogn){
				all_pointslogn.addAll(nets.nodes_MSF);		
	}
	
	EpsilonMST combinedlogn = PrimShortNew(all_pointslogn, Double.MAX_VALUE);
	
	Final_Tree_Edges.addAll(combinedlogn.tree_edges);
	
	System.out.println("combinedlogn (cost of joining) = " + combinedlogn.length);

	System.out.println("lenlogn (from eps-MST's) = " + lenlogn); 
    
	double total_logn = combinedlogn.length  + lenlogn;	
	
	System.out.println("Total Length logn level = " + total_logn);
	
	System.out.println("....... Edges .....");

	double length_sanity_check = 0;
	
	for(TreeEdge ed: Final_Tree_Edges){      
 		edges.add(ed);
        length_sanity_check += 1;
	}
	
	//System.out.println("length sanity = " + length_sanity_check);
	
	long estimatedTime_dis = System.nanoTime() - startTime_dis;

	System.out.println("dis time = " + estimatedTime_dis/1000000000);
	
	System.out.println("End of the Program");

	long startTime_seq = System.nanoTime();    

	if(ii == -1)
	{
	List<Point> points =  PointRDD.collect();
	FileWriter graph_storage = new FileWriter(args[5]);
	
	ArrayList<Point> p = new ArrayList<Point>(points);
	for(int kk = 0; kk < p.size(); kk++){
		graph_storage.write(kk+1 + ";");
		for(int jj = 0; jj < p.get(kk).coordinates.length; jj++){
			
			if(jj == p.get(kk).coordinates.length - 1 ){
				graph_storage.write(p.get(kk).coordinates[jj] + "\n");
			}
			else{
				graph_storage.write(p.get(kk).coordinates[jj] + " ");				
			}
		}
		
		//graph_storage.write('\n');
	}
	
	graph_storage.close();
	
	//System.out.println("length of p = " + p.size());
	
	//EpsilonMST lenr =  PrimShortNew(p, Double.MAX_VALUE);
	
	//ArrayList<Double> tree_edge_len_opt = new ArrayList<Double>(); 
/*	for(TreeEdge e: lenr.tree_edges){
		double dis = distance(e.left, e.right);
		tree_edge_len_opt.add(dis);
	}
*/	
     //Collections.sort(tree_edge_len_opt);
	
     //System.out.println("largest = " + tree_edge_len_opt.get(tree_edge_len_opt.size() - 1));
    
    
     // int size_tree = tree_edge_len_opt.size();
     // FileWriter fw6 = new FileWriter(args[4]);
     
    // for(int jj = 0; jj < size_tree; jj++){
    //	         fw6.write(jj + " , " + tree_edge_len_opt.get(jj) + '\n');   	     	 	 
    // }
     
   // fw6.close(); 
	//double len_reg_mst = lenr.length; 
	//long estimatedTime_seq = System.nanoTime() - startTime_seq;
	//System.out.println("Sequential Time = " + estimatedTime_seq/1000000000);
	//System.out.println("Sequential Length = " + len_reg_mst);
	} 	
	
	System.out.println("time for Distr MST = " + estimatedTime_dis/1000000000);
	//System.out.println("ii = " + ii);
		
 }  // Main Loop Ends .....

 
 //System.out.println("Adding the tree edges to a file");
    
 /*int count = 0;
  
 FileWriter fw7 = new FileWriter(args[3]);
 for(TreeEdge ed: edges){
	 double dis = distance(ed.left, ed.right);
	 fw7.write(ed.left.id + "," + ed.right.id + "," + dis +'\n');
	 count++;
  } */ 
 //fw7.close();
 }
}
