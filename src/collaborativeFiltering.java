import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class collaborativeFiltering {
	HashMap<Integer, Pair<Integer, Integer>> users;
	
	public static void main(String args[]){
		collaborativeFiltering run = new collaborativeFiltering();
	}
	
	public collaborativeFiltering(){
		
		Connection c = null;
	    try {
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:dataSet");
	      c.setAutoCommit(false);
	      Statement stmt = c.createStatement();
	      ResultSet rs = stmt.executeQuery( "SELECT * FROM trainingSet;" );
	      users = new HashMap<Integer, Pair<Integer, Integer>>();
	      //Populate existing results
	      while ( rs.next() ) {
	    	  users.put(rs.getInt("userId"),new Pair<Integer, Integer>(rs.getInt("itemId"), rs.getInt("rating")));
	      }
	      
	      //Get recommended results for each user
	      ArrayList<String> updates = new ArrayList<String>();
	      rs = stmt.executeQuery( "SELECT * FROM testSet;" );
	      while ( rs.next() ) {
	    	  StringBuilder sql = new StringBuilder();
	    	  sql.append("UPDATE testSet set rating = ");
	    	  Integer userId = rs.getInt("userId");
	    	  Integer itemId = rs.getInt("itemId");
	    	  sql.append(getRecommendedRating(userId, itemId));
	    	  sql.append(" WHERE userId = " + userId.toString() + " AND itemId = " + itemId.toString() + ";");
	    	  updates.add(sql.toString());
	      }
	      
	      //Save Data
	      for (String sql : updates){
	    	  stmt.executeUpdate(sql);
	      }

	      c.commit();
	      
	      rs.close();
	      stmt.close();
	      c.close();
	      
	    } catch ( Exception e ) {
	      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      System.exit(0);
	    }
	}
	
	public String getRecommendedRating(Integer user, Integer item){
		String rating = "0";
		
		return rating;
	}
	
	public ArrayList<Integer> findSimilarUsers(Integer limit, ArrayList<Pair<Integer, Integer>> userOne){
		ArrayList<Integer> users = new ArrayList<Integer>();
		
		return users;
		
	}
	
}
