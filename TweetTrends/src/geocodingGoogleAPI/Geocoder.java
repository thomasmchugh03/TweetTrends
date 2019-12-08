package geocodingGoogleAPI;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.errors.ApiException;
import com.google.maps.model.GeocodingResult;

import twitter4j.JSONArray;
import twitter4j.JSONObject;
//@author John Neppel
//SE Practicum 2018-2019
public class Geocoder {
	private static Geocoder instance;
	private String APIKey;
	private GeoApiContext API=null;
	private JSONObject geoResponseHolder; //This will be used to store a valid response returned by the API.
	                                      //This is stored locally here to cut down on the number of requests made to the service.
	
 
	private Geocoder() {
		this.configureSettingsToGoogleAPI();
		this.API = new GeoApiContext.Builder()
				    .apiKey(APIKey)
				    .build();
	}

	
	/*Followed the Singleton Pattern to ensure to prevent
	 * multiple instances of API. This was strongly
	 * recommended in Googles Geocoding API guide.
	 */
	public static Geocoder getInstance() {
		if(instance==null) {
			instance= new Geocoder();
		}
		return instance;
	}
	
	
	/* Method returns a Coordinates Object which contains
	 * latitude and longitude values extracted from the 
	 *'geoResponseHolder' instance variable. This variable
	 * gets initialized in the isLocationValidAndUSOrCA()
	 * method IF the Google API returns valid data within
	 * the United States. This is traditionally bad design
	 * but I wanted to cut down on the number responses made
	 * to the Google API for each parsed Tweet.
	 */
	public Coordinates setCoordinates() {
		Coordinates coords= new Coordinates(0,0);
		double lat=0;
		double longit=0;
		
		if(geoResponseHolder==null) { //double-check that the variable was initialized
			System.out.println("The API did not return a valid response to the queried location");
			System.out.println("Coordinates with Latitude and Longitude of (0,0) were returned");
			return coords;
		}
		JSONObject geometry = geoResponseHolder.getJSONObject("geometry");
		
		//extracts embedded "viewport" JSONObject, according to Google, it
		//contains the recommended viewport for displaying the returned result.
		//It defines the southwest and northeast corners of the viewport bounding box.
		JSONObject viewPortLocation = geometry.getJSONObject("viewport");
		
		JSONObject northeastCoords = viewPortLocation.getJSONObject("northeast");
		JSONObject southwestCoords = viewPortLocation.getJSONObject("southwest");
		
		//Takes the average of between the sets of latitudes and longitudes
		lat = (northeastCoords.getDouble("lat")+ southwestCoords.getDouble("lat"))/2;
		longit = (northeastCoords.getDouble("lng") + southwestCoords.getDouble("lng"))/2;
		
		coords.setLatitude(lat);
		coords.setLongitude(longit);
		this.nullifyGeoResponseHolder();//clears the instance variable to prepare for the next datapoint processing 
		return coords;
	}
	 
	
	/* Method returns the coordinates for the location passed in
	 * via the Google Geocoding API's response. If the location doesn't
	 * correspond to a place that the Google API can find, it will output
	 * output a message stating so and return the generic coordinates: (0,0).
	 */
	public Coordinates returnCoordsForLocation(String location) {
		Coordinates coords= new Coordinates(0,0);
		double lat=0;
		double longit=0;
		JSONObject locationDataResults;
		
		if(this.didAPIgenerateValidResponse(location)==false) { //ensures that the API generates a valid response to location parameter
			System.out.println("The API did not return a valid response to the queried location");
			System.out.println("Coordinates with Latitude and Longitude of (0,0) were returned");
			return coords;
		}
		locationDataResults= this.getAPIResponse(location);
		JSONObject geometry =locationDataResults.getJSONObject("geometry");
		
		//extracts embedded "viewport" JSONObject, according to Google, it
		//contains the recommended viewport for displaying the returned result.
		//It defines the southwest and northeast corners of the viewport bounding box.
		JSONObject viewPortLocation = geometry.getJSONObject("viewport");
		
		JSONObject northeastCoords = viewPortLocation.getJSONObject("northeast");
		JSONObject southwestCoords = viewPortLocation.getJSONObject("southwest");
		
		//Takes the average of between the sets of latitudes and longitudes
		lat = (northeastCoords.getDouble("lat")+ southwestCoords.getDouble("lat"))/2;
		longit = (northeastCoords.getDouble("lng") + southwestCoords.getDouble("lng"))/2;
		
		coords.setLatitude(lat);
		coords.setLongitude(longit);
		this.nullifyGeoResponseHolder();
		return coords;
	}
	
	
	private void configureSettingsToGoogleAPI() {
		try(FileReader fileReader = new FileReader("configuration.ini")) {
			//Gets the API-key value from the file
			Properties properties = new Properties();
			properties.load(fileReader);
			this.APIKey = properties.getProperty("APIKey");
		} catch(Exception e) {
			System.out.println(e);
		}
   }
	
	
	public JSONObject getAPIResponse(String location) {
	    twitter4j.JSONObject APIResponse = null;
		Gson gson = new Gson();
		try {
			GeocodingResult[] responseSet= GeocodingApi.geocode(API, location).await(); //executes call to the Google API to get geo-location data
			
			if(responseSet.length>0) { //if the API does generate coordinates from the location parameter
				String jsonString = gson.toJson(responseSet[0]); //stores API's response as a JSON-String
				APIResponse= new JSONObject(jsonString);
			}
			return APIResponse; //returns the API's response, whether its null or been initialized
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	
	/*Method checks whether the Google API generates a valid
	 *response to the 'location' parameter. It returns true
	 *if the Google API generates a response that isn't null.
	 *This method is necessary because Twitter users, for example,
	 *are able to put whatever text they desire in their bio-locations.
	 *From what I've seen, *most* users will put legitimate
	 *information in their relating to their primary location.
	 *Others, however, will put bullshit text such as 
	 *"Hentai Heaven" (I've actually seen that)
	 * as their bio-location.
	 */
	public boolean didAPIgenerateValidResponse(String location) {
		//null responses from API means that it could not find data about the location
		if(this.getAPIResponse(location)==null) { 
			return false;
		}
		else {
			return true;
		}
	}
	
	
	public boolean isLocationInUSOrCA(String location) {
		if(didAPIgenerateValidResponse(location)==false) {
			System.out.println("The API did not generate a response to the query");
			return false;
		}
		JSONObject locationCoorData= this.getAPIResponse(location);
		JSONArray addressComponents = locationCoorData.getJSONArray("addressComponents"); //extracts the embedded JSON-Array containing address info
		//System.out.println(locationData); //DEBUGGING
		for(int i=0; i<addressComponents.length();i++) {
			JSONObject addressElement =addressComponents.getJSONObject(i); //gets the embedded JSONObject at each element
			String longName = addressElement.getString("longName");
			if(longName.equals("United States")|| longName.equals("Canada")) { //"longName" key contains data about the location's road, town, zipcode, county, state, and/or country,
				return true;
			}
			
		} //end of 'for' loop
		return false;
	}
	
	
	/* This is a combined method of checking whether the Google API's
	 * response is valid and consists of a location within the US or CA.
	 * This was created in order to cut down on the total number of requests made
	 * to the Google Geocoding API. If the String parameter passed into method
	 * does generate a valid response and it is located within the United States
	 * or Canada, it will return true and set the 'geoResponse' instance variable
	 *  equal to it where it will be stored for further processing. This was only 
	 *  done to reduce the amount of requests made to the Google API.
	 */
	public boolean isLocationValidAndUSOrCA(String location) {
		JSONObject APIResponse = this.getAPIResponse(location);
		if(APIResponse==null) { //null API response indicates that location parameter was invalid
			return false;
		}
		
		JSONArray addressComponents = APIResponse.getJSONArray("addressComponents"); //extracts the embedded JSON-Array containing address info components
		for(int i=0; i<addressComponents.length();i++) {
			JSONObject addressElement =addressComponents.getJSONObject(i); //gets the embedded JSONObject at each element
			String longName = addressElement.getString("longName");
			if(longName.equals("United States")||longName.equals("Canada")) { //"longName" key contains data about the location's road, town, zipcode, county, state, and/or country,
				setGeoResponseHolder(APIResponse); //sets the instance variable to API's response.
				return true;
			}
			
		} //end of 'for' loop
		
		return false; //this will be returned if the API generates a valid response but its not within US or CA.
	}
	
	
	/*Sets the instance variable to the API's response so it can be used for
	 *further processing within this class. 
	 */
	private void setGeoResponseHolder(JSONObject APIResponse) {
		this.geoResponseHolder = APIResponse;
	}
	
	
	/* Method sets the 'geoResponseHolder' instance variable to null in order to
	 * ensure its clear for the next location metadata to 
	 * be stored in its place.
	 * 
	 */
	private void nullifyGeoResponseHolder() {
		this.geoResponseHolder=null;
	}
	
	
	/*
	public static void main(String args[]) {
	Geocoder g = new Geocoder();
	String location="Jackson NJ";
	System.out.println(g.getAPIResponse(location));
	  }
	  */
}