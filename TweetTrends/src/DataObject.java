import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import twitter4j.HashtagEntity;
import twitter4j.JSONObject;
import twitter4j.Query;
import twitter4j.Status;

public abstract class DataObject {
	protected DataArchival archiver; //responsible for getting the connection to the database
	
	
	
/* Every extension of the DataObject class must
 * generate its own SQL statement(s) to be executed
 * based on its data components that will be archived
 * accordingly to the database.
 */
public abstract void executeAllQueries();





}
 