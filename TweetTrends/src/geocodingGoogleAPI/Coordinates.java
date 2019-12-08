package geocodingGoogleAPI;
//@author John Neppel
	//SE Practicum 2018-2019
public class Coordinates {
	private double latitude;
	private double longitude;

	public Coordinates(double lat, double longit) {
		latitude= lat;
		longitude= longit;
	}
	
	public Coordinates() {
		this.latitude=0;
		this.latitude=0;
	}
	public void setLatitude(double value) {
		this.latitude= value;
	}
	
	public double getLatitude() {
		return this.latitude;
	}
	
	public void setLongitude(double value) {
		this.longitude= value;
	}
	
	public double getLongitude() {
		return this.longitude;
	}
	
	public String toString() {
		return "Latitude: " + latitude
				+ " Longitude: " + longitude;
	}
}
