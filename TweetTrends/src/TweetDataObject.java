import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import twitter4j.HashtagEntity;
import twitter4j.JSONArray;
import twitter4j.JSONObject;
import twitter4j.Query;
import twitter4j.Status;

import geocodingGoogleAPI.Coordinates;

//@author John Neppel, Thomas McHugh
	//SE Practicum 2018-2019
public class TweetDataObject extends DataObject {
	private long TweetID; //the unique ID number associated with each Tweet
	private String TweetText; //the text of the Tweet itself
	private Coordinates coordinates;
	private Timestamp tweet_timestamp; //when the Tweet was shared YYYY-MM-DD HH:MM:SS
	private HashtagEntity[] arrayOfHashtags; //an array of hashtags that are located within each Tweet object
	
	private String queryTagText; //the text of the query
	
	
	
    /*This constructor is for archiving Tweet/Status objects that CONTAIN
     * geo-location metadata.
     */
	public TweetDataObject(Status Tweet, Query q, JSONObject TweetAsJSON) {
		//pulls the appropriate data fields from the Tweet and Query Objects
		this.TweetID = Tweet.getId();
		this.TweetText = this.setTweetText(TweetAsJSON); //sets the text of the Tweet accordingly
		
		java.util.Date referenceDate= Tweet.getCreatedAt(); //creates a Java 'Date' object from the method within the Status class
		this.tweet_timestamp = fromDateToTimestamp(referenceDate); //passes the Java 'Date' object into method which returns a sql.timestamp object
		
		this.arrayOfHashtags = Tweet.getHashtagEntities(); //an array of hashtags within the Tweet
		queryTagText= q.getQuery(); //gets the String of the tag portion of the query
		
		this.coordinates = new Coordinates();
		this.coordinates= setCoordinates(TweetAsJSON);  //sets the coordinates for the Tweet object by extracting it from the JSONObject
		
		this.archiver = DataArchival.getInstance(); //instantiates the variable inherited from the super class
	}
	
	 
	/* This constructor is for archiving Tweet/Status objects that DO NOT CONTAIN
	 * geo-location metadata. Take note of the 'coords' parameter passsed into this
	 * constructor. The latitude/longitude encapsulated within this parameter have 
	 * been generated outside of this class via the Google Geocoding API. 
	 * 
	 */
	public TweetDataObject(Status Tweet, Query q, JSONObject TweetAsJSON, Coordinates coords) {
		//pulls the appropriate data fields from the Tweet and Query Objects
				this.TweetID = Tweet.getId();
				this.TweetText = this.setTweetText(TweetAsJSON);//sets the text of the Tweet accordingly
				
				java.util.Date referenceDate= Tweet.getCreatedAt(); //creates a Java 'Date' object from the method within the Status class
				this.tweet_timestamp = fromDateToTimestamp(referenceDate); //passes the Java 'Date' object into method which returns a sql.timestamp object
				
				this.arrayOfHashtags = Tweet.getHashtagEntities(); //an array of hashtags within the Tweet
				queryTagText= q.getQuery(); //gets the String of the tag portion of the query
				
				this.coordinates= coords; //sets the coordinates via the parameter passed into constructor
				this.archiver = DataArchival.getInstance(); //instantiates the variable inherited from the super class
	}


	/*Method calls upon each 'generateQuery' method within the class to execute the
	 *queries that they create sequentially. They must be executed in the
	 *order specified below due to foreign key constraints
	 */
	@Override
	public void executeAllQueries() {
       this.generateTweetTagsQuery();
       this.generateTweetDataQuery();
       this.generateAssocTagsDescipQueries();
       this.generateAssocTagsQueries();
	}
	

	/* Generates a comma-separated-value string based on the hashtags of the Tweet/Status object.
	 * 
	 */
	public String HashtagsCSVString() {
		String HashtagsCSV= "";
		for(int currIndex=0; currIndex< arrayOfHashtags.length; currIndex++) {
		if(currIndex<arrayOfHashtags.length-1) { //as long as the current Index is NOT the last index of the array
		 HashtagsCSV=  HashtagsCSV + arrayOfHashtags[currIndex].getText().toLowerCase() + ","; //Append to the string with the hashtag-text and a comma
		}
		else {
		HashtagsCSV = HashtagsCSV+ arrayOfHashtags[currIndex].getText().toLowerCase(); //the last value from the array doesn't have a comma after it.
		}
	}
		return HashtagsCSV;
	}

	
	/*Method takes in a java.util.Date object as a parameter and creates a
	 *representative sql.timestamp object based on the data within the parameter.
	 *This is done so the timestamp of the Tweet can be effectively stored in the
	 *MySQL Database Table.
	 */
	public Timestamp fromDateToTimestamp(java.util.Date dateObject) {
		//gets the number of milliseconds since January 1, 1970, 00:00:00 GMT represented by this Date object.
		long milliseconds = dateObject.getTime();
		Timestamp ts = new Timestamp(milliseconds); //passes in the variable that corresponds to the value passed into Timestamp constructor
		return ts;
	}
	
	 
	/* Method uses certain instance variables from this class
	 * to generate an insert query for the tweet_tags table.
	 * It will then execute the generated query.
	 */
	private void generateTweetTagsQuery() {
		try {
		PreparedStatement statement1= null;
		//if the parameter within 'VALUES' is identical to an existing QueryTagText (an unique key), the insert portion is ignored
		String insert1 = "INSERT IGNORE INTO tweet_tags(QueryTagText) VALUES(?);"; 
		
		statement1 = archiver.getConnectionToDatabase().prepareStatement(insert1);
		statement1.setString(1, queryTagText);
		statement1.executeUpdate();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
	}
	
	
	/* Method uses all of the metadata pulled from
	 * the Tweet and coordinate objects to generate an insert query
	 * for archiving data to the tweet_data table.
	 * It will then execute the generated query.
	 */
	private void generateTweetDataQuery() {
		PreparedStatement statement2= null;
		String insert2 = "INSERT INTO tweet_data(TweetID, TweetText, query_tag_id, latitude, longitude, tweet_timestamp, Hashtags)"+ 
	            " VALUES(?,?,(SELECT Query_Tag_ID FROM tweet_tags WHERE QueryTagText= ?),?,?,?,?);";
		
		try {
		statement2= archiver.getConnectionToDatabase().prepareStatement(insert2);
		statement2.setLong(1, TweetID);
		statement2.setString(2, TweetText);
		statement2.setString(3, queryTagText);
		statement2.setDouble(4, this.coordinates.getLatitude());
		statement2.setDouble(5, this.coordinates.getLongitude());
		statement2.setTimestamp(6, tweet_timestamp);
		statement2.setString(7,this.HashtagsCSVString());
		
		statement2.executeUpdate(); //executes the query statement
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	
	/* Method generates insert queries for the associated_tags_description table
	 * using the text from the query and the array of hashtags associated with the
	 * Tweet. It will then generate the executed queries.
	 */
	private void generateAssocTagsDescipQueries() {
		String queryTextWithoutHashtag= removeHashtagFromString(queryTagText); //this stores the text of the query which will be used for comparison
		String insert3 = "INSERT IGNORE INTO associated_tags_description(tag_text) VALUES(?);";
		
		for (int currIndex = 0; currIndex < arrayOfHashtags.length; currIndex++) {
			try {
				PreparedStatement statement= null;
				String currHashtagText = arrayOfHashtags[currIndex].getText().toLowerCase(); //gets the Hashtag text at the current index of the array
				if(queryTextWithoutHashtag.equalsIgnoreCase(currHashtagText)) {//if the text of the query matches the text of the hashtag at the current index
				  continue; //returns the execution back to the 'for' loop, we do not want to archive an associated hashtag that corresponds to the hashtag within the query
				}
			    statement = archiver.getConnectionToDatabase().prepareStatement(insert3);
				statement.setString(1, currHashtagText); //sets the parameter as the Hashtag text at the current index of the array
				statement.executeUpdate(); //executes the query statement
			} 
			catch (Exception e) {
				System.out.println(e);
			}
		}
	}
	
	
	/* Method generates insert queries for the tweet_tags table
	 * using the text from the query, the ID from the Tweet, and
	 * the array of hashtags associated with the Tweet. It will then
	 * execute the generated queries.
	 */
	private void generateAssocTagsQueries() {
		String queryTextWithoutHashtag= removeHashtagFromString(queryTagText); //this stores the text of the query (without any '#') which will be used for comparison
		String queryString = "INSERT INTO associated_tags(associated_tag_id, TweetID) "+
		"VALUES((SELECT ID FROM associated_tags_description WHERE tag_text=?), ?);";
		
		for (int currIndex = 0; currIndex < arrayOfHashtags.length; currIndex++) {
			try {
				PreparedStatement statement= null;
				String currHashtagText = arrayOfHashtags[currIndex].getText(); //gets the Hashtag text at the current index of the array
				if(queryTextWithoutHashtag.equalsIgnoreCase(currHashtagText)) {
					continue; //returns the execution to the 'for' loop; hashtags that are identical to the query tag are NOT archived
				}
			    statement = archiver.getConnectionToDatabase().prepareStatement(queryString);
			    statement.setString(1, currHashtagText);
				statement.setLong(2, TweetID); //sets the parameter to the TweetID number
				statement.executeUpdate(); //executes the query statement
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
	
	
	
	/* Method takes in a String parameter, checks to see if
	 * the first character of it contains a '#' and returns the string
	 * without the beginning '#' (if it contains it.) 
	 * If the parameter doesn't contain a '#' as the first character,
	 * the string is simply returned unmodified. This method is necessary
	 * because the twitter4j.HashtagEntity object will return the text of
	 * the hashtag within the Tweet without the '#'. That means in order
	 * to compare the hashtagEntities within a Tweet to a query searching
	 *  for a particular hashtag (e.g. #Metoo) the '#' needs to be removed.
	 * 
	 */
	private String removeHashtagFromString(String hashtagKeyword ) {
		String modifiedString = "";
		if(hashtagKeyword.charAt(0)=='#') {
			modifiedString=hashtagKeyword.substring(1,hashtagKeyword.length()); //creates a substring without '#'
			modifiedString = modifiedString.toLowerCase();
			return modifiedString; //returns the String without the '#'
		}
		else {
			return hashtagKeyword;
		}
	}
	
	private Coordinates setCoordinates(JSONObject TweetAsJSON) {
		double lat=0;
		double longit=0;
		int numOfGeoPoints=0; //keeps track of the number of Geo-coordinate pairs within the JSON-Tweet object (should always be 4)
		JSONObject placeData = new JSONObject(); //will store 'place' data of the Tweet (if necessary)
		Coordinates coords = new Coordinates();
		
		if(TweetAsJSON.has("geoLocation")) { //SOME Tweets contain the 'geoLocation' key, which is an exact location data point
			JSONObject geoLocationPoint= TweetAsJSON.getJSONObject("geoLocation"); //extracts the embedded geoLocation JSONObject
			
			//System.out.println(geoLocationPoint.toString()); //DEBUGGING
			lat= geoLocationPoint.getDouble("latitude");
			longit=geoLocationPoint.getDouble("longitude");
			coords.setLatitude(lat);
			coords.setLongitude(longit);
		}
		
		else {	
		try {
		 placeData = TweetAsJSON.getJSONObject("place"); //attempts to get the 'place' JSONObject which contains geo-data.
		}
		catch(Exception e) {
			System.out.println(e);
			return coords; //if an error is thrown, end further execution of the method
		}
		
		 //ALL Tweets with geo-coordinate metadata have a bounding-box, which is a set of geo-coordinate pairs surrounding the location
		JSONArray boundingBoxCoord = placeData.getJSONArray("boundingBoxCoordinates");
		for(int i=0; i< boundingBoxCoord.length();i++) {
			JSONArray coordArray = boundingBoxCoord.getJSONArray(i); //gets the embedded array object
			for(int j=0; j<coordArray.length(); j++) {
		    JSONObject coordElement = coordArray.getJSONObject(j);//gets the JSONObject at the current index
			lat = lat + coordElement.getDouble("latitude");
			longit= longit + coordElement.getDouble("longitude");
			numOfGeoPoints++;
			}
		} 
		 lat = lat/numOfGeoPoints; //takes the average of all the latitude points
		 longit = longit/numOfGeoPoints; //takes the average of all the longitude points
		 coords.setLatitude(lat);
		 coords.setLongitude(longit);
		 
		} //end of 'else'
		
		return coords;
	}
	
	/*Method extracts the text of the Tweet and returns it.
	 * If the Tweet is a retweet of another Status, the 
	 * method will ensure that the returned text of the 
	 * Tweet will contain "RT" and the @username, along with the
	 * full text of the Tweet.
	 */
	public String setTweetText(JSONObject TweetAsJSON) {
		String tweetText="";
		
		//Checks to see if the Tweet is a 'retweet' of another Tweet. Tweets that are
		//retweeted are automatically truncated by the Twitter Search API. In order to get the
		//full non-truncated Tweet, we need to extract the embedded 'retweetedStatus' JSONObject.
		
		if(TweetAsJSON.getString("text").substring(0, 3).equals("RT ")&& TweetAsJSON.has("retweetedStatus")) { //double check that it contains the embedded JSONObject 
			JSONObject retweetedStatus = TweetAsJSON.getJSONObject("retweetedStatus"); //the Tweet that was retweeted by the user
			String originalStatus= TweetAsJSON.getString("text"); 
			
			for(int i=0; i<originalStatus.length();i++) { //This produces a substring encompassing the "RT" and the @username: whose Tweet was retweeted
				tweetText= tweetText + originalStatus.charAt(i)+"";
				if(originalStatus.charAt(i)==':') { 
					break; //a colon represents the end of the @username
				}
			}
			
			tweetText= tweetText + " "+ retweetedStatus.getString("text"); //Appends the full text of the retweet
		} 
		
		else { //if the Tweet is not a Retweet
			tweetText=TweetAsJSON.getString("text"); //simply get the text of the Tweet
		}
		return tweetText;
	}
	
	
	
	
}
