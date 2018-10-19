
public class Point 
{ 
	  public double x; 
	  public double y; 
	  
	  public Point(String point)
	  {
		  String[] coordinates = point.split(" ");
		  this.x = Double.parseDouble(coordinates[0]);
		  this.y = Double.parseDouble(coordinates[1]);
	  }
	  
	  public Point(double x, double y) { 
	    this.x = x; 
	    this.y = y; 
	  } 
	  
	  public String toString()
	  {
		  return Double.toString(this.x) + " " + Double.toString(this.y);
	  }
	  
	  public double calcDistance(Point p)
	  {
		  double dx = this.x - p.x;
		  double dy = this.y - p.y;
		  
		  return Math.sqrt(dx * dx + dy * dy);
	  }
} 