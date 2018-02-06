//package MST;



public class Point implements java.io.Serializable {

	
	private static final long serialVersionUID = 1L;
	double[] coordinates;
	int connected_component;
	int dimension;
	int id;
	
	Point(int d, double[] cordinate, int component, int name){
		dimension = d;
		coordinates = cordinate;
		connected_component = name -1;
		id = name;
	}
}
