import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import geocodingGoogleAPI.Coordinates;
import twitter4j.TwitterException;
//@author Thomas McHugh and John Neppel
	//SE Practicum 2018-2019
public class DataArchival {
	private String url;
	private String username;
	private String password;
	private Connection connection;
	private static DataArchival instance;
	
	
	/* Constructor */
   private DataArchival() {
	   configureSettingsToDatabase(); //initializes the connection settings via the .ini file
	   this.connection = configureConnectionToDatabase(); //sets up the connection using configured settings
   }
   
   public static DataArchival getInstance() {
	   if(instance==null) {
		   instance = new DataArchival();
		   return instance;
	   }
	   else {
		   return instance;
	   }
   }
   
   
   /*Method reads in the configuration.ini file to get the connectivity
    * settings for the MySQL tweet_trends Database schema
    */
   private void configureSettingsToDatabase() {
		try(FileReader fileReader = new FileReader("configuration.ini")) {
			//Gets the data parameters necessary for connecting from the database
			Properties properties = new Properties();
			properties.load(fileReader);
			url= properties.getProperty("url");
			username = properties.getProperty("username");
			password = properties.getProperty("password");

			
		} catch(Exception e) {
			System.out.println(e);
		}
   }
	
   
	//Returns the configured database connection object
	public Connection getConnectionToDatabase() {
		return this.connection;
	}
	
	
	/*Uses the url, username and password instance variables to set up connection to the
	 * database
	 */
	private Connection configureConnectionToDatabase() { 
		try {
			Connection con = DriverManager.getConnection(
					url, username, password); // connects to server using url, username, password.
																										
			return con;
		} catch (Exception e) {
			System.out.println(e);
			return null;
		}
	}
	
	
	
	/*Method queries the database for the largest TweetID associated with the
	 *text of a query tag (e.g. #Metoo, #Hello, or any other query). It returns
	 *the result of the query, which may be the largest Tweet ID number in
	 *response to the executed query, or NULL if nothing is found. This method
	 *is to help avoid archiving duplicated Tweets associated with a particular query.
	 * 
	 * Tweet ID's are sequential, meaning the most recently shared ones are larger
	 * than the older ones.
	 */
	public Object getLargestAssocTweetID(String queryTagText) {
    String statement = "SELECT max(TweetID) FROM tweet_data WHERE(query_tag_id= (SELECT Query_Tag_ID FROM tweet_tags WHERE QueryTagText=?))";
    Object queryResult= null;
    
    try {
    PreparedStatement query= connection.prepareStatement(statement);
    query.setString(1, queryTagText); //adds the parameter to the query-string
    ResultSet resultSet= query.executeQuery();
    
    if(resultSet.next()) { //if there is anything within the result set
    queryResult=resultSet.getObject(1); //the executed query will only cotain
    }
    
    }
    catch(Exception e) {
    	System.out.println(e);
    	return null;
    }
	return queryResult;
	}
	
	
	
	/*Method queries the database for the smallest TweetID associated with the
	 *text of a query tag (e.g. #Metoo, #Hello, or any other query). It returns
	 *the result of the query, which may be the smallest Tweet ID number in
	 *response to the executed query, or NULL if nothing is found. This method
	 *is to help avoid archiving duplicated Tweets associated with a particular query.
	 * 
	 * Tweet ID's are sequential, meaning the most recently shared ones have larger ID's
	 * than the older ones.
	 */
	public Object getSmallestAssocTweetID(String queryTagText) {
	    String statement = "SELECT min(TweetID) FROM tweet_data WHERE(query_tag_id= (SELECT Query_Tag_ID FROM tweet_tags WHERE QueryTagText=?))";
	    Object queryResult= null;
	    
	    try {
	    PreparedStatement query= connection.prepareStatement(statement);
	    query.setString(1, queryTagText); //adds the parameter to the query-string
	    ResultSet resultSet= query.executeQuery();
	    
	    if(resultSet.next()) { //if there is anything within the result set
	    queryResult=resultSet.getObject(1); //the executed query will only cotain
	    }
	    
	    }
	    catch(Exception e) {
	    	System.out.println(e);
	    	return null;
	    }
		return queryResult;
		}
	
	
	
	/* Method checks whether connection to the database
	 * is active and successful. Returns true if it is actively
	 * connected to the database and false otherwise.
	 */
	public boolean isDatabaseConnectionActive() {
		if(this.getConnectionToDatabase()==null) {
			return false;
		}
		else {
			return true;
		}
	}
	
	
	public boolean isLocationDataArchived(String bioLocation) {
		String queryStatement= "SELECT EXISTS(SELECT* FROM archived_location_data WHERE LocationName=?);";
	    try {
	    	PreparedStatement query = this.connection.prepareStatement(queryStatement);
	    	query.setString(1, bioLocation);
	    	ResultSet result = query.executeQuery();
	    	result.next(); //there will always be a result in response to query; either 1 or 0
	    	if(result.getBoolean(1)==true) {
	    		return true;
	    	}
	    	else {
	    		return false;
	    	}
	    }
		catch(Exception e) {
			System.out.println(e);
			return false;
		}
	    
	    
	}
	
	public Coordinates getCoordsFromArchivedLocationData(String bioLocation) {
    	Coordinates coords= new Coordinates();
    	double lat=0;
    	double longit=0;
    	String queryStatement= "SELECT latitude, longitude FROM archived_location_data WHERE LocationName=?;";
    	try {
    		PreparedStatement query = connection.prepareStatement(queryStatement);
    		query.setString(1, bioLocation);
    		ResultSet result = query.executeQuery();
    		if(result.next()) {
    			lat= result.getDouble(1);
    			longit= result.getDouble(2);
    		}
    		coords.setLatitude(lat);
    		coords.setLongitude(longit);
    		return coords;
    	}
    	catch(Exception e) {
    		System.out.println(e);
    		return coords;
    	}
    	
    	
    }
	
	
	/* Method takes in a String representation of a location name and a Coordinates object. It will first check the database
	 * to see if a row already exists with that location name because it is a unique index. Then it will insert the
	 * location name and the latitude and longitude associated with the Coordinates object into the database.
	 * 
	 */
	public void archiveLocationData(String bioLocation, Coordinates generatedCoords) {
		if(this.isLocationDataArchived(bioLocation)==true) { //first level check to see if location data was previously archived
			System.out.println("Location data has already been archived!");
		}
		
		
		String queryStatement = "INSERT IGNORE INTO archived_location_data(LocationName, latitude, longitude) VALUES(?,?,?);";
		try {
			PreparedStatement query = connection.prepareStatement(queryStatement);
			query.setString(1, bioLocation);
			query.setDouble(2, generatedCoords.getLatitude());
			query.setDouble(3, generatedCoords.getLongitude());
			query.executeUpdate();
			
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
	}
	
	
	public void closeDatabaseConnection() {
		try {
		this.connection.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	
	public static void main(String args[]) throws TwitterException
	{
	    DataArchival archiver = DataArchival.getInstance();
		Coordinates coords=archiver.getCoordsFromArchivedLocationData("West Long Branch");
		System.out.println(coords);
		System.out.println(archiver.isLocationDataArchived("FRUITLDJFLK"));
		
		   
	}
	   
}



//https://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html