import java.util.Scanner;

import twitter4j.Query;
import twitter4j.TwitterException;
import java.util.regex.*;
//@author Thomas McHugh, John Neppel
	//SE Practicum 2018-2019
public class TweetTrendsAdminDriver {
	 static Scanner console;
	 
	@SuppressWarnings("unused")
	public static void main(String args[]) throws TwitterException
	{
		TweetTrendsCore app = new TweetTrendsCore();
	    boolean stop= false;
	    console = new Scanner(System.in);
	    
	    System.out.println("\n" + "**Welcome to the Tweet Trends Administrative Interface**");
	 
	   do {
	     String sinceDate="";
	     String untilDate="";
	 	 String queryStatement="";
	 	 int numericalInput;
	 	
		System.out.println("Enter 1 to archive an historical array of Tweet metadata points related to a query ");
		System.out.println("Enter 2 to archive the most recently shared Tweets relating to a query");
		System.out.println("Enter 3 to print the results of a query to the Search Tweets API" );
		System.out.println("Enter 4 to exit");
		
		numericalInput = getNumericalInput();
		
		switch(numericalInput) {
		case 1:
			queryStatement= getQueryStatement();
			promptForSinceDate();
			sinceDate= getValidDate();
			
			promptForUntilDate();
			untilDate= getValidDate();
			app.archiveRangeOfTweets(queryStatement, sinceDate, untilDate);
			break;
		
		case 2:
			queryStatement= getQueryStatement();
			app.archiveMostRecentTweets(queryStatement);
			break;
			
		case 3:
			queryStatement= getQueryStatement();
			promptForSinceDate();
			sinceDate= getValidDate();
			
			promptForUntilDate();
			untilDate= getValidDate();
			System.out.println("RESULTS:" + "\n");
			app.printSomeQueryResults(queryStatement, sinceDate, untilDate, 1000);
			break;
			
		case 4:
			stop=true; //ends execution of system
			app.closeDatabaseConnection(); //ensures that database connection has been closed
			break;
			
			default:
				System.out.println("One of the options wasn't entered");
				break;
		}
		
	    } while(stop==false);
	    
	    
	    console.close();
	    
	} //end of main
	
	
	public static String getQueryStatement() {
		String input="";
		System.out.println("Enter a query statement to be executed to the API");
		input = console.nextLine();
		while(input.equals("")|| input.equals(null)) {
			System.out.println("A query statement wasn't entered, try entering another one");
			input= console.nextLine();
		}
		return input;
	}
	
	
	
	//yyyy-mm-dd
	public static String getValidDate() {
	String inputDate= "";
	Pattern p = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"); //regular expression to test any date with yyyy-mm-dd format                                                                          //entered dates that DO NOT exist (e.g 2015-02-33) queried to the API simply returns NULL results 
	
	inputDate = console.nextLine();
	Matcher m = p.matcher(inputDate); //tests the user input against the regular expression
	boolean doesItMatch= m.matches();
	
	while(doesItMatch==false) {
		System.out.println("An invalid date was entered." +
	" Enter one with the following format: yyyy-mm-dd "+ "\n");
		
	inputDate=console.nextLine(); //takes in user's input once again
	m = p.matcher(inputDate);
	if(m.matches()) { //re-tests the user input against the regex
		doesItMatch=true;
	}
	
	}//end of 'while' loop
	return inputDate;
	}
	
	
	public static void promptForSinceDate() {
		System.out.println("Enter a 'Since' Date for the Query to the Twitter Search API ");
		System.out.println("Date must be in the following format: yyyy-mm-dd" );
	}
	
	
	public static void promptForUntilDate() {
		System.out.println("Enter an 'Until' Date for the Query to the Twitter Search API ");
		System.out.println("Date must be in the following format: yyyy-mm-dd" );
	}
	
	
	public static int getNumericalInput() {
		String input="";
		int numericalInput=0;
		try {
			input = console.nextLine();
			numericalInput= Integer.parseInt(input);
		}
			catch(Exception e) {
				System.out.println("A numerical value was not entered.");
			}
		return numericalInput;
	}
}
