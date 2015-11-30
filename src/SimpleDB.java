
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//Credit: https://github.com/renataghisloti/Recommender-System-SlopeOne-with-Movielens-Dataset/blob/master/SlopeOne.java
public class SimpleDB {
	//change to the DB
	final String connection_string = "jdbc:sqlite:main.db";
	public Connection c;
	
	Map<Integer,Map<Integer,Integer>> itemMatrix;
	Map<Integer,Map<Integer,Integer>> userMatrix;
	Map<Integer,Map<Integer,Integer>> predMap;
	Map<Integer,Map<Integer,Float>> differenceMap;
	Map<Integer,ArrayList<Integer>> isNeeded;

	public SimpleDB() {
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(connection_string);
			c.setAutoCommit(false);
			System.out.println("Opened database successfully");
			
			//creates the predictions table if it does not exist
			Statement predStat = c.createStatement();
			predStat.executeUpdate("CREATE TABLE IF NOT EXISTS Predictions(UserID INT,ItemID INT,Rating REAL);");
			predStat.close();
			
			
			Statement diff = c.createStatement();
			diff.executeUpdate("CREATE TABLE IF NOT EXISTS Diff(i INT,j INT,Value REAL)");
			diff.close();
			
			c.commit();
			
			
		} catch (Exception e) {
			error(e);
		}
	}
	
	public void count(){
		try{
			Statement sStat = c.createStatement();
			ResultSet rs = sStat.executeQuery("SELECT count(*) from Diff");
			while(rs.next()){
				System.out.println(rs.getInt(1));
				
			}
			System.out.println("Done");
			sStat.close();
		}catch (Exception e) {
			error(e);
		}
	}
	//used for generating the difference matrix
	public void populateItemMap(){
		itemMatrix = new HashMap<Integer,Map<Integer,Integer>>();
		try{
			Statement sStat = c.createStatement();
			ResultSet rs = sStat.executeQuery("SELECT * FROM Training_Set");
			while(rs.next()){
				int user = rs.getInt(1);
				int item = rs.getInt(2);
				int rating = rs.getInt(3);
			
				itemMatrix.putIfAbsent(item, new HashMap<Integer,Integer>());
				itemMatrix.get(item).put(user, rating);
				
			}
			System.out.println("Done");
			sStat.close();
		}catch (Exception e) {
			error(e);
		}
		
	}
	//gets all the users that need to have predictions
	private void populateUserMap(){
		userMatrix = new HashMap<Integer,Map<Integer,Integer>>();
		try{
			Statement sStat = c.createStatement();
			ResultSet rs = sStat.executeQuery("SELECT * FROM Test_Set");
			while(rs.next()){
				int user = rs.getInt(1);
				int item = rs.getInt(2);
				int rating = rs.getInt(3);
			
				userMatrix.putIfAbsent(user, new HashMap<Integer,Integer>());
				userMatrix.get(user).put(item, rating);
				
			}
			System.out.println("User Matrix Complete");
			sStat.close();
		}catch (Exception e) {
			error(e);
		}
	}
	//this contains all the rated items from all the users that we are predicting for
	private void populatePredMap(){
		predMap = new HashMap<Integer,Map<Integer,Integer>>();
		try{
			Statement select = c.createStatement();
			ResultSet rs = select.executeQuery("SELECT * FROM Training_Set");
			while(rs.next()){
				int user = rs.getInt(1);
				int item = rs.getInt(2);
				int rating = rs.getInt(3);	
				
				predMap.putIfAbsent(user, new HashMap<Integer,Integer>());
				predMap.get(user).put(item, rating);
			}
			System.out.println("Pred Map Complete");
			select.close();
		}catch (Exception e) {
			error(e);
		}
	}
	
	private void fillDifferences(int lower,int upper){
		try{
			Statement select = c.createStatement();
			ResultSet rs = select.executeQuery("SELECT * FROM Diff");

			while(rs.next()){
				int i = rs.getInt(1);
				int j = rs.getInt(2);
				float value = rs.getFloat(3);
				
				if(isNeeded.containsKey(i)){
					if(isNeeded.get(i).contains(j)){
						differenceMap.putIfAbsent(i, new HashMap<Integer,Float>());
						differenceMap.get(i).put(j, value);
					}
					
				}
				
			}

			select.close();
			
		}catch (Exception e){
			error(e);
		}
	}
	private void isNeeded(){
		isNeeded = new HashMap<Integer,ArrayList<Integer>>();
		for(int cUser: userMatrix.keySet()){
			for(int unratedItem: userMatrix.get(cUser).keySet()){
				
				if(predMap.containsKey(cUser)){
					for(int knownItem: predMap.get(cUser).keySet()){
						isNeeded.putIfAbsent(knownItem, new ArrayList<Integer>());
						isNeeded.get(knownItem).add(unratedItem);
					}
				}
				
			}
		}
		System.out.println("isNeeded Done!");
		
	}

	public void predictRest(){
		this.populatePredMap();
		this.populateItemMap();
		int count = 0;
		System.out.println("Starting to predict the rest");
		try{
			PreparedStatement update = c.prepareStatement("UPDATE Predictions SET Rating = ? WHERE UserID = ? AND ItemID = ?");
			Statement select = c.createStatement();
			ResultSet rs = select.executeQuery("SELECT * FROM Predictions WHERE Rating = 0.0");
			
			while(rs.next()){
				int userId = rs.getInt(1);
				int itemId = rs.getInt(2);
				
				update.setInt(2, userId);
				update.setInt(3, itemId);
				
				if(predMap.containsKey(userId)){
					float total = 0;
					int freq = 0;
					for(int cItem: predMap.get(userId).keySet()){
						float rating = predMap.get(userId).get(cItem).floatValue();
						total = total + rating;
						freq++;		
					}
					update.setFloat(1, total/freq);
				}else{
					float total = 0;
					float freq =0;
					if(itemMatrix.containsKey(itemId)){
						for(int cUser: itemMatrix.get(itemId).keySet()){
							float rating =itemMatrix.get(itemId).get(cUser).floatValue();
							total = total + rating;
							freq++;
						}
						update.setFloat(1, total/freq);
					}else{
						update.setFloat(1, 3);
					}
				}
				count++;
				update.executeUpdate();
				
				if(count == 1000){
					c.commit();
					count = 0;
				}
				
				
			}
			c.commit();
			select.close();
			update.close();
		}catch(Exception e){
			error(e);
		}
	}

	public void predict(){
		int count = 0;
		differenceMap = new HashMap<Integer,Map<Integer,Float>>();
		
		//fills all the main hashmaps
		this.populateUserMap();
		this.populatePredMap();
		this.isNeeded();
		
		int lower = 0;
		int upper = 100000;
		
		for(int i = 0; i < 9000; i ++){
			count++;
			if(count == 100){
				System.out.println(differenceMap.size());
				count = 0;
			}
			fillDifferences(lower,upper);
			lower = lower + 100000;
			upper = upper + 100000;
		}
		
		System.out.println("Predictions Starting");
		
		count = 0;
		try{

			PreparedStatement insert = c.prepareStatement("INSERT INTO Predictions VALUES (?,?,?)");
			for(int cUser: userMatrix.keySet()){
				insert.setInt(1, cUser);
				for(int unratedItem: userMatrix.get(cUser).keySet()){
					float total = 0;
					float freq = 0;
					int knownItemRating = 0;
					float average = 0;
					insert.setInt(2, unratedItem);
					
					if(predMap.containsKey(cUser)){
						for(int knownItem: predMap.get(cUser).keySet()){
							knownItemRating = predMap.get(cUser).get(knownItem).intValue();
							
							if(differenceMap.containsKey(knownItem) && differenceMap.get(knownItem).containsKey(unratedItem)){
								float diff = differenceMap.get(knownItem).get(unratedItem).floatValue();
								total = total + diff + knownItemRating;
								freq++;
								
							}
							
						}
					}
					
					average = total / freq;
					count++;
					if(freq == 0 || ( average  > 5 || average < 1)){
						insert.setFloat(3, 0);
					}else{
						
						insert.setFloat(3, average);
					}
					
					insert.executeUpdate();
					if(count == 1000){
						c.commit();
						count = 0;
					}
					
				}
				
			}
			c.commit();
			System.out.println("Predictions Done!");
			insert.close();
		}catch (Exception e){
			error(e);
		}
		
		
	}
	public void calcDiff(){
		int count = 0;
		try{
			PreparedStatement insert = c.prepareStatement("INSERT INTO Diff VALUES (?,?,?)");
			//goes through all the items in the userMatrix
			for(int i: itemMatrix.keySet()){
				insert.setInt(1, i);
				Map<Integer,Integer> mapOne = itemMatrix.get(i);
				//works out all the differences
				for(int j: itemMatrix.keySet()){
					if(i != j){
						float difference = calc(mapOne,itemMatrix.get(j));
						insert.setInt(2, j);
						insert.setFloat(3, difference);
						insert.executeUpdate();
						count++;
						if(count == 1000){
							count = 0;
							c.commit();
						}
							
					}
				}
			}
			c.commit();
			insert.close();
			System.out.println("Finished!!!!!");
		}catch(Exception e){
			error(e);
		}
		
	}
	private float calc(Map<Integer,Integer> one,Map<Integer,Integer> two){
		float difference = 0;
		float freq = 0;
		if(one.size() <= two.size()){
			for(int user: one.keySet()){
				if(two.containsKey(user)){
					freq++;
					difference = difference + (one.get(user).intValue() - two.get(user).intValue());
				}
			}
		}else{
			for(int user: two.keySet()){
				if(one.containsKey(user)){
					freq++;
					difference = difference + (one.get(user).intValue() - two.get(user).intValue());
				}
			}
		}
		return difference / freq;
	}


	public void error(Exception e) {
		System.err.println(e.getClass().getName() + ": " + e.getMessage());
		System.exit(0);
	}

	


}