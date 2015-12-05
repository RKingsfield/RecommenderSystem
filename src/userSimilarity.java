
public class userSimilarity {
	pearsonDB db = new pearsonDB();
	public static void main(String[] args) {
		userSimilarity test = new userSimilarity();
		test.init();
	}
	//designed to be done in two stages
	private void init(){
		//stage 1
		db.populateUserMap();
		db.calcSimm();
		//stage 2
		db.predict();
		//db.predictRest();
		
		
	}
}
