import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.Query;
import twitter4j.TwitterException;

import java.sql.SQLException;
import java.util.*;

//@author Thomas McHugh, John Neppel
	//SE Practicum 2018-2019
public class TweetTrendsCore {

	private DataArchival archiver;
	private DataRetrieval retriever;
	
	public TweetTrendsCore() {
		archiver = DataArchival.getInstance();
		retriever = new DataRetrieval();
	}
	
	
	/* Method will archive the most recently shared Tweets on Twitter pertaining to the query,
	 * up until it parses a Tweet that has an ID less than or equal to the ID of the most recenthly
	 * archived Tweet located within the database.
	 */
	public void archiveRangeOfTweets(String queryStatement, String sinceDate, String untilDate) {
		if(archiver.isDatabaseConnectionActive()==false) {
			this.printDatabaseConnectionError();
			return;
		}
		
		if(this.isDateValidFormat(sinceDate)==false|| this.isDateValidFormat(untilDate)==false) {
			System.out.println("Date is NOT in yyyy-mm-dd format; no metadata will be parsed or archived from the entered query");
			return;
		}
		twitter4j.Query generatedQuery= retriever.createQuery(queryStatement, sinceDate, untilDate);
		try {
			retriever.parseTweetData(generatedQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	/*Method will archive Tweets pertaining to the query, starting with the most recently shared ones on Twitter,
	  up UNTIL it reaches a Tweet that has an ID less than a previously archived Tweet. As a reminder, Tweet ID's are sequential.
	  The newest Tweets have an ID that is greater than the older ones. By doing this, the newly shared 
	  Tweets on Twitter are archived and ones that have already been previously parsed/archived are not stored once again.
	  This prevents storing redundant data.
	 */
	public void archiveMostRecentTweets(String queryStatement) {
		if(archiver.isDatabaseConnectionActive()==false) {
			this.printDatabaseConnectionError();
			return;
		}
		
		long minimumID=0; //the ID of the most recently archived Tweet pertaining to the query
		
		//The "until" date, as part of the Twitter Query Object, is non-inclusive.
		//Therefore, setting the "until" date for tomorrow returns the most recent results from today's date.
		Calendar tomorrowsDate= this.getTomorrowsDate(); 
		Calendar aMonthsAgoDate= this.getDateFromMonthAgo();//This will be used as an arbitrary "since" date.
		String sinceDate = this.convertDateToFormattedString(aMonthsAgoDate);
		String untilDate = this.convertDateToFormattedString(tomorrowsDate);
		
		if(archiver.getLargestAssocTweetID(queryStatement)==null) { //if no Tweets have already been archive pertaining to the query
			System.out.println("There are currently no Tweets archived within the database "
					+ "pertaining to the query: "+ queryStatement);
			System.out.println("Therefore the most recently shared Tweets will NOT be archived"+ "\n");
			return;
		}
		else {
			minimumID= (long) archiver.getLargestAssocTweetID(queryStatement);//gets the ID of the most recent Tweet that has been previously archived
		}
	
		Query query= retriever.createQuery(queryStatement, sinceDate, untilDate, minimumID); //this query will cover the most recently shared Tweets, up until the most recently archived one.
		try {
			retriever.parseTweetData(query);
			this.archiver.getConnectionToDatabase().close(); //closes the connection
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	/*Ensures that the date is in yyyy-mm-dd format. Returns true if parameter passed in is in that format
	 *and false otherwise.
	 */
	public boolean isDateValidFormat(String inputDate) {
		Pattern p = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");  // regular expression to test any date with yyyy-mm-dd format
																					// //entered dates that DO NOT exist
																					// (e.g 2015-02-33) queried to the
																					// API simply returns NULL results

		Matcher m = p.matcher(inputDate); // tests the user input against the regular expression
		boolean doesItMatch= m.matches();
		
			if (doesItMatch==true) { // tests the user input against the regex
				return true;
			}
			else {
				return false;
			}

		} 
	
	
	public java.util.Calendar getTomorrowsDate() {
		Calendar tomorrowsDate = Calendar.getInstance();
	    tomorrowsDate.add(Calendar.DATE, 1); //advances to the date of one day later than the current date
	    
	    return tomorrowsDate;
	}
	
	
	public java.util.Calendar getDateFromMonthAgo() {
		Calendar oneMonthAgosDate = Calendar.getInstance();
	    oneMonthAgosDate.add(Calendar.MONTH, -1); //subtracts one month from the current Date
	    
	  return oneMonthAgosDate;
	}
	
	
	/*Method takes in a Calendar object and returns a representative string in yyyy-mm-dd format.
	 * This is to ensure that all generated dates are in that format in order to effectively query the
	 * Twitter API.
	 */
	public String convertDateToFormattedString(Calendar date) {
		String formattedDate="";
		//Java returns the numerical value associated with each month by the Gregorian and Julian standard.
		//This means that the numerical value of each month is ONE-less than our traditional calendar.
		int month = date.get(Calendar.MONTH)+1;
		String monthString= month+ "";
		
		int dayOfMonth= date.get(Calendar.DAY_OF_MONTH);
		String dayOfMonthString=dayOfMonth+"";
		
		if(month<10) { // Any Date values less than 10 are given a 0 before it (part of formatting)
			monthString= "0"+ month;
		}
		
		if(dayOfMonth<10) {
			dayOfMonthString="0"+ dayOfMonthString;
		}
		
		//yyyy-mm-dd formatted string
		formattedDate= date.get(Calendar.YEAR)+"-" + monthString +
				"-"+ dayOfMonthString;
		
		return formattedDate;
	}
	
	
	public void printSomeQueryResults(String queryStatement, String sinceDate, String untilDate, int amountPrinted) {
		if(this.isDateValidFormat(sinceDate)==false|| this.isDateValidFormat(untilDate)==false) {
			System.out.println("Date is NOT in yyyy-mm-dd format; no metadata will be printed from the entered query");
			return;
		}
		
		Query createdQuery = retriever.createQuery(queryStatement, sinceDate, untilDate); //creates a Twitter4J.Query object from parameters
		try {
		retriever.printQueryResults(createdQuery, amountPrinted);
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	
	
	private void printDatabaseConnectionError() {
		System.out.println("Not connected to the database; no data will be parsed"
				+ " or archived");
	}
	
	public void closeDatabaseConnection() {
		this.archiver.closeDatabaseConnection();
	}
	
	
	

}
