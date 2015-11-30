import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class computeMatrix {
	
	HashMap<Integer, ArrayList<Pair<Integer, Integer>>> users;
	
	public static void main(String args[]){
		computeMatrix compute = new computeMatrix();
	}
	
	public computeMatrix(){
		Connection c = null;
	    try {
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:dataSet");
	      c.setAutoCommit(false);
	      Statement stmt = c.createStatement();
	      ResultSet rs = stmt.executeQuery( "SELECT * FROM trainingSet;" );
	      
	      users = new HashMap<Integer, ArrayList<Pair<Integer, Integer>>>();
	      //Populate existing users
	      while ( rs.next() ) {
	    	  Pair<Integer, Integer> newPair = new Pair<Integer, Integer>(rs.getInt("itemId"), rs.getInt("rating"));
	    	  int userId = rs.getInt("userId");
	    	  if(users.containsKey(userId)){
	    		  users.get(userId).add(newPair);
	    	  } else {
	    		  ArrayList<Pair<Integer, Integer>> newList = new ArrayList<Pair<Integer, Integer>>();
	    		  newList.add(newPair);
	    		  users.put(userId, newList);
	    	  }
	      }

	      rs.close();
	      stmt.close();
	      System.out.println("Populated users list.");
	      
	      stmt = c.createStatement();
	      String sql = "CREATE TABLE similarity " +
                  "(userId INT NOT NULL," +
                  " otherUserId INT NOT NULL, " + 
                  " similarity REAL NOT NULL,"
                  + "PRIMARY KEY (userId, otherUserId));";
	      stmt.executeUpdate(sql);
	      stmt.close();
	      System.out.println("Created table.");
	      
	      stmt = c.createStatement();
	      for (Integer mainUserId : users.keySet()){
			for (Integer userId : users.keySet()){	
				  sql = "INSERT INTO similarity (userId, otherUserId, similarity) VALUES (" + mainUserId + ", " + userId + ", " + calculateSimilarity(users.get(mainUserId), users.get(userId)) + ");";
				  //System.out.println(sql);
			      stmt.executeUpdate(sql);
			      
			}
	      }
	      stmt.close();
	      c.commit();
	      c.close();
	      
	    } catch ( Exception e ) {
	      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      System.exit(0);
	    }
	    
	}
	
	//Find similarity between two users
	public Double calculateSimilarity(ArrayList<Pair<Integer, Integer>> userOne, ArrayList<Pair<Integer, Integer>> userTwo){
		if (userOne == userTwo) return 0.0; 

		Double similarity = 0.0;
		ArrayList<Integer> jointItems = getJointlyRatedItems(userOne, userTwo);
		if (jointItems.size() > 0) {
			Double userOneAvg = getAverageRating(userOne);
			Double userTwoAvg = getAverageRating(userTwo);
			Double oneSquaredTotal = 0.0;
			Double twoSquaredTotal = 0.0;
			Double topTotal = 0.0;
			
			for(Integer item : jointItems){
				Integer userOneRating = userOne.get(userOne.indexOf(new Pair<Integer, Integer>(item, -1))).getRight();
				Integer userTwoRating = userTwo.get(userTwo.indexOf(new Pair<Integer, Integer>(item, -1))).getRight();
				
				Double one = userOneRating - userOneAvg;
				Double two = userTwoRating - userTwoAvg;
				topTotal += one*two;
				oneSquaredTotal += one*one;
				twoSquaredTotal += two*two;
				
			}
			
			if(oneSquaredTotal == 0 || twoSquaredTotal ==0) return 0.0;
					
			similarity = topTotal / (Math.sqrt(oneSquaredTotal) * Math.sqrt(twoSquaredTotal));
			
		}
		return similarity;
	}
	
	//Gets a list of all itemIds that are shared by two users
	public ArrayList<Integer> getJointlyRatedItems(ArrayList<Pair<Integer, Integer>> userOne, ArrayList<Pair<Integer, Integer>> userTwo){

		ArrayList<Integer> jointItems = new ArrayList<Integer>();
		for (Pair<Integer, Integer> ratings : userOne){
			int currentItem = ratings.getLeft();
			
			if (userTwo.contains(new Pair<Integer, Integer>(currentItem, -1))){
				jointItems.add(currentItem);	

			}
		}
		return jointItems;
	}
	
	//Finds the average rating a user gives
	public Double getAverageRating(ArrayList<Pair<Integer, Integer>> user){
		Double total = 0.0;
		for (Pair<Integer, Integer> ratings : user){
			total += ratings.getRight();
		}
		Double avg = (total/user.size());
		return avg;
	}
	
}

class Pair<L,R> {

	  private final L left;
	  private final R right;

	  public Pair(L left, R right) {
	    this.left = left;
	    this.right = right;
	  }

	  public L getLeft() { return left; }
	  public R getRight() { return right; }

	  @Override
	  public int hashCode() { return left.hashCode() ^ right.hashCode(); }

	  @Override
	  public boolean equals(Object o) {
	    if (!(o instanceof Pair)) return false;
	    Pair pairo = (Pair) o;
	    //If right is less than 0, then only check if left matches.
	    if ((Integer) this.right < 0) return this.left.equals(pairo.getLeft());
	    
	    return this.left.equals(pairo.getLeft()) &&
	           this.right.equals(pairo.getRight());
	  }

}