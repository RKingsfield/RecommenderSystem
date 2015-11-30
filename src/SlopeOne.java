

public class SlopeOne {
	SimpleDB db = new SimpleDB();
	public static void main(String[] args) {
		SlopeOne test = new SlopeOne();
		test.init();
	}
	//designed to be done in two stages
	private void init(){
		//stage 1
		db.populateItemMap();
		db.calcDiff();
		//stage 2
		db.predict();
		db.predictRest();
		
		
	}
	


}	
