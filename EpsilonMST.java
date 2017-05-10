package MST;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.clearspring.analytics.util.Pair;


public class EpsilonMST implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	double length;
	List<Pair<Point, Point>> edges;
	List<Point> singletons;
	int number_of_connected_components;

	
	EpsilonMST(){
		singletons = new ArrayList<Point>();
	}
	
}
