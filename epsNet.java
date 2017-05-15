package MST;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.List;



public class epsNet implements Serializable  {

	private static final long serialVersionUID = 1L;

	double length_MSF;
	ArrayList<Point> nodes_MSF;
	List<TreeEdge> edges;
	epsNet(){
		nodes_MSF = new ArrayList<Point>();
		edges = new ArrayList<TreeEdge>();
	}

}
