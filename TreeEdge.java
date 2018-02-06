//package MST;

import java.io.Serializable;

public class TreeEdge implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Point left;
	Point right;
	int value;
	
    TreeEdge()
    {
       
        value = 3;
    }

	
	   public boolean equals(Object obj){
	        
	        //System.out.println("Inside Equals");
	        if (obj instanceof TreeEdge) {
	            TreeEdge T2 = (TreeEdge) obj;
	            if((this.left.id == T2.left.id && this.right.id==T2.right.id) ||(this.left.id == T2.right.id && this.right.id == T2.left.id))
	                return true;
	        }
	        return false;
	    }
	   
	   public int length(Point p1, Point p2){
		   
		   int len = 0;
		   int dimension = p1.dimension;
			for(int d = 0; d < dimension; d++){
				len += (p1.coordinates[d] - p2.coordinates[d]) * (p1.coordinates[d] - p2.coordinates[d]);
			}
		   
		   return len;
	   }
	   
	    public int hashCode() {
	        //System.out.println("In hashcode "+"value is :"+this.empName);
	        int hash = 3;
	        hash = 7 * hash + this.left.id * this.right.id * this.value;
	        return hash;
	    }
	
	Point left(){
		return left;
	}
	
	Point right(){
		return right;
	}
}
