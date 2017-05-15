package MST;

import java.io.Serializable;

public class TreeEdge implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Point left;
	Point right;

	Point left(){
		return left;
	}
	
	Point right(){
		return right;
	}
}
