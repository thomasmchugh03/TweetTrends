import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


import java.awt.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

import twitter4j.HashtagEntity;
import twitter4j.JSONArray;
import twitter4j.JSONObject;
import twitter4j.Location;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.RateLimitStatusEvent;
import twitter4j.Status;

import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileReader;
import java.sql.SQLException;
import java.util.Properties;

import geocodingGoogleAPI.Coordinates;
import geocodingGoogleAPI.Geocoder;
//@author John Neppel, Thomas McHugh
	//SE Practicum 2018-2019
public class DataRetrieval  {
	
	private final  int MAX_NUM_OF_DATA_POINTS; //maximum number of data points per batch for the version of the API
	private Twitter API;
	private Geocoder GoogleAPI;
	private DataArchival archiver;
 
	//@author John Neppel, Thomas McHugh
	//SE Practicum 2018-2019
	public DataRetrieval() {
		API = configConnectionToAPI(); //sets up the connection to the Twitter API service
		MAX_NUM_OF_DATA_POINTS= getMaxNumOfDataPointsPerRequest();
		GoogleAPI = Geocoder.getInstance();
		archiver = DataArchival.getInstance(); //this will just be used to determine whether certain location data-points have been already stored in the db.
	}
	
	
	/* Method takes in a query statement in the form of a string, along with 
	 * a 'Since' and an 'Until' Date to adjust the time range that the query
	 * will represent. It will build a Twitter4j.Query object based on the parameters
	 * passed in and return the generated query object.
	 */
	public Query createQuery(String queryStatement, String sinceDate, String untilDate) {
		Query generatedQuery= new Query(queryStatement);
		generatedQuery.setCount(MAX_NUM_OF_DATA_POINTS);
		generatedQuery.setSince(sinceDate); //yyyy-mm-dd format
		//Non-inclusive of the date set in "until" (result-data generated from the query will start the latest at 11:59PM one day previous of the 'until date')
		generatedQuery.setUntil(untilDate);
		return generatedQuery; //returns the generated Query object
	}
	
	
	public Query createQuery(String queryStatement, String sinceDate, String untilDate, long miniumTweetID) {
		Query generatedQuery= new Query(queryStatement);
		generatedQuery.setCount(MAX_NUM_OF_DATA_POINTS);
		generatedQuery.setSince(sinceDate); //yyyy-mm-dd format
		//Non-inclusive of the date set in "until" (result-data generated from the query will  start the latest at 11:59PMone day previous of the 'until date')
		generatedQuery.setUntil(untilDate);
		generatedQuery.setSinceId(miniumTweetID); //Tweets with an ID lower than this value will NOT be returned by this executed query
		return generatedQuery; //returns the generated Query object
	}
	
	
	/* Method takes in a Query-object and executes it to the Twitter API.
	 * Once the query is executed, the Twitter API returns the Tweet statuses in
	 * a series of batches. From each Tweet that is parsed, it is passed through
	 * a series of gateways to determine whether it will be archived and which
	 * TweetDataObject constructor to use to pass the data along. TweetDataObjects
	 * will contain all the objects relevant to be further parsed through and archived.
	 *
	 */
	public void parseTweetData (Query query) throws TwitterException, SQLException {
		
		if(this.areAvailableRequestsDepleted()) { //first-level check to see if requests to the API can be executed
			System.out.println("No more requests to the API can be made for the following"
					+ " amount of seconds " + getSecondsUntilAPIReset());
			return;
		}
		
		
		Query referenceQuery = query; //creates a reference to the query passed into method so it can be modified when necessary
		QueryResult result = API.search(referenceQuery); ///executes the query through the API
		ArrayList<Status> statuses = (ArrayList<Status>) result.getTweets(); //list is initialized to the list of Tweets returned by 'getTweets'
		
		for(int index=0; index<statuses.size();index++) {
			Status TweetAtCurrIndex= statuses.get(index);
			JSONObject JSONTweet = new JSONObject();
			JSONTweet = this.toJSONObject(TweetAtCurrIndex);
			//this.printTweetInJSON(TweetAtCurrIndex); DEBUGGING
			
			
			//The following is a series of gateways to determine if the Tweet/Status object should be archived.
			//It also determines which TweetDataObject constructor to use in order to parse/archive data or
			//whether the the Twitter-user's bioLocation should be archived or retrieved for processing.
			//I know it's ugly.
			if(this.isTweetInEnglishOrSpanishOrFrench(JSONTweet)) {
				 //*****************************************************************************************//BEGIN Outer-Most IF
				if(this.doesTweetMetadataContainGeoLocation(JSONTweet)&& this.isTweetCountryUSOrCA(JSONTweet)) { //for Tweets with embededed US/CA coordinate metadata
					//creates a local TweetDataObject which will contain all relevant Tweet/Query info to be archived
					TweetDataObject object1 = new TweetDataObject(TweetAtCurrIndex, referenceQuery, JSONTweet);
					object1.executeAllQueries(); //executes all the queries to the database
					System.out.println("A Tweet was archived. Coordinates were set via embedded metadata.");
				}
				else
					//--------------------------------------------------------------------------------------------//BEGIN OUTER ELSE
					if(this.doesUserHaveBioLocationInfo(JSONTweet)) { //BEGIN INNER IF
						String bioLocation = this.getUserBioLocationInfo(JSONTweet);
						Coordinates generatedCoords;
						if(archiver.isLocationDataArchived(bioLocation)) { //if the bio-location coordinate data has been previously archived
							generatedCoords = archiver.getCoordsFromArchivedLocationData(bioLocation);
							TweetDataObject tweet = new TweetDataObject(TweetAtCurrIndex, referenceQuery, JSONTweet, generatedCoords);
							tweet.executeAllQueries();
							System.out.println("A Tweet was archived. Coordinates were set via"
									+ " location data that was previously archived");
						}
						//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
						else
						if(GoogleAPI.isLocationValidAndUSOrCA(bioLocation)) {
								 generatedCoords= GoogleAPI.setCoordinates(); //sets the coordinates that relate to the bioLocation
								 if(this.isGeneratedCoordsDefaultUS(generatedCoords)) {
									 break; //breaks out of the outter if
								 }
								 archiver.archiveLocationData(bioLocation, generatedCoords); //archives the bio-location string and coords for future reference
								TweetDataObject object2 = new TweetDataObject(TweetAtCurrIndex, referenceQuery,
										JSONTweet, generatedCoords);
								object2.executeAllQueries();
								System.out.println("A Tweet was archived. Coordinates were set via Google Geocoding API");
						}
					} //END INNER IF
				//*****************************************************************************************//END Outer-Most-IF, END OUTER ELSE
					}
			
			
			if(index==(statuses.size()-1)) { //if the 'for' loop is on the last index of the ArrayList
				if(this.areAvailableRequestsDepleted()) { //checks to see if anymore requests through the API can be made
					this.pauseExecutionOfSystem(); //makes the system rest, if necessary
				}
				
				referenceQuery.setMaxId(statuses.get(index).getId()-1); //modifies the query so it doesn't take in anymore Tweets with an ID greater than the last Tweet in the current batch
				result = API.search(referenceQuery); //re-executes the query with the updated MaxID
				statuses.clear();//clears the contents of the ArrayList so it can be re-filled with the next batch of Tweets
				statuses = (ArrayList<Status>)result.getTweets();
				index=0; //resets the 'for' loop
			}
				
		} //end of 'for' loop
		this.archiver.getConnectionToDatabase().close();
		
}
		
	
	
	public void printQueryResults(Query query, int maxNumOfPrintedResults) throws TwitterException {
		if(this.areAvailableRequestsDepleted()) { //first-level check to see if requests to the API can be made
			System.out.println("No more requests to the API can be made for the following "
					+ "amount of seconds " + getSecondsUntilAPIReset());
		   return;
		}
		
		Query searchQuery = query; //creates a reference to the query passed into method so it can be modified
		QueryResult result = API.search(searchQuery); //executes the query through the  API
		int count=1; //keeps count of how many statuses are outputted
	    ArrayList<Status>statuses = (ArrayList<Status>) result.getTweets(); //the ArrayList is initialized to the list of Tweet statuses given by the result 
	    
	    
			for(int index=0; index<statuses.size();index++) {
				Status currTweet = statuses.get(index);
				
				this.printTweetInJSON(currTweet);
				
				JSONObject ob = this.toJSONObject(currTweet);
				System.out.println("User Bio-location: " + this.getUserBioLocationInfo(ob));
				// System.out.println("Has embedded geo-location: " + this.doesTweetMetadataContainGeoLocation(ob)); //DEBUGGING
				System.out.println(count);
				

				if (count==maxNumOfPrintedResults) { //checks to see if the number of results outputted matches the maximum parameter
					return;
				}
				
				if(index==(statuses.size()-1)) {  //if the 'for' loop is on the last index of the ArrayList
					if(this.areAvailableRequestsDepleted()) { //checks to see if anymore requests can be generated in response to queries
						this.pauseExecutionOfSystem(); //makes the system rest, if necessary
					}
					
					searchQuery.setMaxId(statuses.get(index).getId()-1); //modifies the query so it doesn't take in anymore Tweets with an ID greater than the last Tweet in the batch
					statuses.clear();  //clears the contents of the ArrayList so it can be re-filled with the next batch of Tweets
					result = API.search(searchQuery);  //re-executes the with the modified query
					statuses = (ArrayList<Status>) result.getTweets(); //gives the ArrayList the data from the re-executed query.
					index=0; //resets the for' loop to iterate through the result of the updated query
				}
				count++;
			} //end of 'for' loop
			
		}
		
			
   
	
	/*Method configures the connection to the Twitter API service using the unique
	 * access tokens and keys given to the developers. It returns a configured Twitter object
	 * in which all service calls to the API will be conducted through.
	 */
	private Twitter configConnectionToAPI() {
		ConfigurationBuilder config = new ConfigurationBuilder(); //sets up a configuration builder for the API settings
		config.setDebugEnabled(true);
		config.setJSONStoreEnabled(true); //modifies the configuration to allow for it to return data in JSON

		 //Reads in the configuration.ini file to load the API authorization keys and tokens
		try(FileReader fileReader = new FileReader("configuration.ini")) {
			Properties properties = new Properties();
			properties.load(fileReader);
			
			//authorization keys and tokens within configuration.ini are supplied by Twitter Developer API/------------
			config.setOAuthConsumerKey(properties.getProperty("OAuthConsumerKey"));
			config.setOAuthConsumerSecret(properties.getProperty("OAuthConsumerSecret"));
			config.setOAuthAccessToken(properties.getProperty("OAuthAccessToken"));
			config.setOAuthAccessTokenSecret(properties.getProperty("OAuthAccessTokenSecret"));
			
			//config.setRestBaseURL("https://api.twitter.com/1.1/tweets/search/30day/TweetGeoCodesDev.json");
		}
		catch(Exception e) {
			System.out.println(e);
			return null;
		}
		
		TwitterFactory twitFactory = new TwitterFactory(config.build()); 
		Twitter twit = twitFactory.getInstance(); //builds an instance of the API through the configuration settings
		return twit; //returns the Twitter API object, commands to the API will be carried out through this
	}
	
	
	
	public int getMaxNumOfDataPointsPerRequest() {
		int numberFromFile=0; //temporarily initialized
		 //Reads in the configuration.ini file
		try(FileReader fileReader = new FileReader("configuration.ini")) {
			Properties properties = new Properties();
			properties.load(fileReader);
			
			//retrieves the maximum number of data points that can be retrieved per request to the API
			numberFromFile = Integer.parseInt(properties.getProperty("MAX_NUM_OF_DATA_POINTS"));
			return numberFromFile;
		}
		catch(Exception e) {
			System.out.println(e);
		}
		return numberFromFile;
	}
	
	
	/* Method checks whether all the data requests generated
	 * by the API (which are in response to queries) have
	 * been used up. It returns true if the API has been
	 * queried for the maximum amount for a time threshold 
	 * and false otherwise.
	 */
	public boolean areAvailableRequestsDepleted() {
		//don't want to exceed having less than '8' request remaining (it will sometimes break otherwise for some reason)
		if(this.getRemainingAmountOfRequests()<=8) { 
			return true;
		}
		else {
			return false;
		}
	}
	
	
	
	/*The Twitter Search API can only be queried for a limited number of times per certain time threshold.
	 * This function returns the number of requests remaining for the time being.
	 */
	public int getRemainingAmountOfRequests() {
		try {
			Map<String, RateLimitStatus> limits = API.getRateLimitStatus(); // maps between the services offered by the API by their name and the corresponding rateLimit object
																	
			RateLimitStatus service = limits.get("/search/tweets"); // Gets the RatelimitStatus object details for the "search Tweets" API service
																
			// the API can be queried for a limited number of requests per a certain time-threshold
			return service.getRemaining(); /// returns the remaining amount of requests that can be executed for a time threshold
		    }
		
		   catch (Exception e) {
			System.out.println(e);
			return 0;
		}
	}
	
	
	
	/* The Twitter API can be queried only for a limited number of times
	 * per set time-threshold (which is predetermined by the API).
	 *  Once the API is queried the maximum number of allowed times
	 *  for the time-threshold, a certain amount of rest time needs
	 * to pass before more queries are written to the API.
	 * Method returns the total amount of seconds that need to pass
	 * until the API can fully reset.
	 */
	public int getSecondsUntilAPIReset() {
		try {
			Map<String, RateLimitStatus> limits = API.getRateLimitStatus(); //maps between the services offered by the API (by their name) and the corresponding rateLimit object
			RateLimitStatus service = limits.get("/search/tweets"); //Gets the RatelimitStatus object details for the "search Tweets" API service
			
			//the API can be queried for a limited number of requests per a certain time-threshold// 
			return service.getSecondsUntilReset()+20; // returns the total amount of seconds that needs to pass until the API can be queried again
			}
			catch(Exception e) {
		      System.out.println(e);
		      return 800; //if any error occurs from attempting to retrieve the number of seconds, make the system rest for 15 minutes default
			}
	}
	
	
	/* The Twitter API offers numerous different
	 * services to be accessed by the application.
	 * Method outputs the remaining amount of requests
	 * available for each service.
	 */
	public void printAPIServicesLimitAmounts() {
	   try {
		Map<String, RateLimitStatus> rateLimit = API.getRateLimitStatus(); //maps between the services offered by the API by their name and the corresponding rateLimit object for 
		for(String s: rateLimit.keySet()) {
			System.out.println(s + " " + rateLimit.get(s).getRemaining() ); //prints the name of each service along with the number of requests that can be made
		}
	   }
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	
	
	/* Method takes in a Tweet Status object and uses
	 * the Google.gson library to output the data
	 * in JSON format. This library worked much 
	 * better than Twitter4J's JSON library
	 */
	public void printTweetInJSON(Status status) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(status); //creates a JSON-formatted String from the Tweet status object 
		 System.out.println(jsonString); 
	}
	
	
	/*Method converts a Tweet/Status object into a corresponding JSONObject.
	 * This is done to allow for the data contained within to be parsed and processed.
	 */
	public JSONObject toJSONObject(Status status) {
		Gson gson = new Gson();
		String jsonString = gson.toJson(status);
	    JSONObject statusAsJSON = new JSONObject(jsonString);
	    
		return statusAsJSON;
	}
	
	
	/*When called, this method pauses the execution of the system.
	 * It gets the number seconds that the system needs to pause
	 * from the Twitter Search API. The number of seconds represents
	 * how much time needs to elapse until requests to the API can
	 * be made once again. In the case that more requests to the API are
	 * generated when it needs time to reset, an error would be thrown.
	 */
	private synchronized void pauseExecutionOfSystem() {
		//gets the number of seconds until reset-time from the API and adds 20 seconds more for safety
		int secondsUntilReset = this.getSecondsUntilAPIReset(); 
		System.out.println("**System is now going to sleep for " + secondsUntilReset + " seconds until the API resets**" +
		"\n");
		try {
			TimeUnit.SECONDS.sleep(secondsUntilReset); //makes the whole system rest for the amount of time set by the API until reset
			System.out.println("Rebooting now");
		} catch (InterruptedException e) {
			System.out.println(e);
		} 
	}

	
	/* Method takes in a JSONObject representation of a Tweet/Status
	 * Object, parses it, and determines whether it is written in English
	 * or Spanish. It returns true if the "lang" key (ISO 639-1) contained
	 * within the JSON represents the language-code for English or Spanish
	 *  or French and false otherwise. These languages were selected because
	 *  they are the top 3 most common throughout the US/Canada. Also, other
	 *  languages (e.g Chinese) would output through the Twitter4j library as
	 *  "???".
	 */
	public boolean isTweetInEnglishOrSpanishOrFrench(JSONObject ob) {
		//System.out.println("Language: " +ob.get("lang")+ "\n"); //TESTING
		if(ob.get("lang").equals("en")|| ob.get("lang").equals("es")
				|| ob.get("lang").equals("fr")) { //all Tweets have a metadata-point denoting its language
			return true;
		}
		else {
			return false;
		}
	}
	
	
	/* This method first checks whether the Tweet (passed in as a representative JSONObject)
	 * contains geo-location metadata. If it contains geo-location metadata, the method will
	 * check whether the Tweet was shared in Canada or the United States. It returns true if 
	 * it a Tweet that was shared within those countries. It returns false otherwise if the Tweet
	 * wasn't shared in those countries OR if it doesn't contain geo-coordinate metadata at all.
	 */
	public boolean isTweetCountryUSOrCA(JSONObject ob) {
		JSONObject placeData = new JSONObject();
		//System.out.println("Above Tweet has 'place:' " + ob.has("place")); // TESTING 
		
		if(ob.has("place")) { //some Tweets have the "place" geo-coordinate key
		placeData = ob.getJSONObject("place"); //extracts the embedded user JSON data for place
		String countryCode = placeData.getString("countryCode");
		//System.out.println("countryCode is "+ placeData.getString("countryCode")); //TESTING
		
		if(countryCode.equalsIgnoreCase("US")||countryCode.equalsIgnoreCase("CA")) {
			return true;
		}
		
		else { //if it has a country-code but it's NOT US or CA
			return false;
		}
		
		}
		
		else { //if the Tweet contains NO geo-location data at all
			//System.out.println("The above Tweet DOES NOT contain place/location data");
			return false;
		}
		
	}
		
	
	/* Twitter has the functionality of allowing users to manually enter their location into their Twitter-bios.
	 * This method checks whether the user has entered any sort of data in there. Returns true if the user has
	 * entered some sort of information into there and false otherwise.
	 */
	public boolean doesUserHaveBioLocationInfo(JSONObject ob) {
		if(ob.has("user")) {
			JSONObject userData = ob.getJSONObject("user"); //extracts the user-data JSONObject
			String userBioLocation= userData.getString("location");
			if(userBioLocation.equals("")|| userBioLocation.equals(null)) { //if the user's bio-location is empty
				return false;
			}
			else { //if the user has inputted data into their bio-location
				return true;
			}
		}
		else { //if the Tweet-object has no "user" metadata (which it always should)
			return false;
		}
	}
	
	
	public String getUserBioLocationInfo(JSONObject ob) {
		JSONObject userData = ob.getJSONObject("user"); //extracts the user-data JSONObject
		String userBioLocation= userData.getString("location");
		return userBioLocation;
	}
	
	public boolean doesTweetMetadataContainGeoLocation(JSONObject ob) {
		if(ob.has("place")) {
			return true;
		}
		else {
			return false;
		}
	}
	
	
	/* If the Twitter User puts anything pertaining to "US" or "United States"
	 * in their bio-location, the generated coordinates in response to that
	 * String will always be 37.60000000, -95.66500000. Since alot of users
	 * set their bio-locations to that String, it leads to alot of datapoints 
	 * with the database with those coordinates, leading to inaccurate recordings.
	 * This method is to combat that by checking whether the generated coordinates
	 * match this pair.
	 * 
	 */
	public boolean isGeneratedCoordsDefaultUS(Coordinates coords) {
		if((coords.getLatitude()==37.6) && (coords.getLongitude()==-95.66499999999999)) {
			return true;
		}
		else {
			return false;
		}
	}
	

/*
public static void main(String args[]) throws TwitterException
{   
	DataRetrieval retrieval = new DataRetrieval();
	Query samplequery = retrieval.createQuery("#Metoo", "2018-12-10", "2018-12-14"); //yyyy-mm-dd format
	System.out.println(retrieval.getRemainingAmountOfRequests() + " requests remaining");
	System.out.println(retrieval.getSecondsUntilAPIReset()+ " seconds until reset");
    retrieval.parseTweetData(samplequery);
}
*/
	
}



//The Tweet ID is sequential, meaning the MOST RECENT Tweets has a larger ID than the older ones before it. The last (oldest) Tweet
	//returned in each Result-batch has the smallest ID, therefore to get more Tweets with the same query, we need to set the ID of the
	//last Tweet as the MaxID of the query.

/* Important Links and notes
 * http://twitter4j.org/javadoc/index.html
 * https://developer.twitter.com/en/docs/tweets/search/overview
 * https://developer.twitter.com/en/pricing
 * https://developer.twitter.com/en/docs/tweets/search/api-reference/premium-search#Authentication
 * First Geo-tagged Tweets: 11/19/2009
 * Profile Geo enrichment metadata and filtering: 2/17/2015
 * 
 * 
 * 
 */