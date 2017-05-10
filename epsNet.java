package MST;

import java.io.Serializable;
import java.util.ArrayList;

public class epsNet implements Serializable  {

	private static final long serialVersionUID = 1L;

	double length_MSF;
	ArrayList<Point> nodes_MSF;
	
	epsNet(){
		nodes_MSF = new ArrayList<Point>();
	}

}
