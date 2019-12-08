//@author Thomas McHugh, John Neppel
	//SE Practicum 2018-2019
public class MeTooDataUpdater {

	public static void main(String[] args) {
		TweetTrendsCore application = new TweetTrendsCore();
		application.archiveMostRecentTweets("#Metoo");
		application.closeDatabaseConnection(); //ensures that database connection has been closed
		
 
	}
  
}
