

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class DerbyMethods {
	
	/*Insert records into derby database
	 * @list list of business names objects to be added to database
	 */
	public static void insertDerby(ArrayList<Record> list){
		String derby="jdbc:derby:businessnames";
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat formatter = new SimpleDateFormat(pattern);
		
		try{
			
			//Establish connection
			Connection con= DriverManager.getConnection(derby);
			Statement state=con.createStatement();
			
			//For each business name in list
			for(Record name: list){
				int res;
				res=state.executeUpdate("INSERT INTO BusinessNames(name) VALUES('"+name.getBus_name()+"')");
				//Insert record if values from table have been set
				//ABN
				if(name.getAbn()!=null){
					res=state.executeUpdate("INSERT INTO BusinessNumbers(name,abn) "
							+ "VALUES('"+name.getBus_name()+"','"+name.getAbn()+"')");
				}
				//Status
				if(name.getStatus()!=null){
					res=state.executeUpdate("INSERT INTO RegistrationStatus(name,status) "
							+ "VALUES('"+name.getBus_name()+"','"+name.getStatus()+"')");
				}
				//Registration date
				if(name.getReg_date()!=null){
					res=state.executeUpdate("INSERT INTO RegistrationDates(name,registrationDate) "
							+ "VALUES('"+name.getBus_name()+"','"+formatter.format(name.getReg_date())+"')");
				}
				//Renewal date
				if(name.getRen_date()!=null){
					res=state.executeUpdate("INSERT INTO RenewalDates(name,renewalDate) "
							+ "VALUES('"+name.getBus_name()+"','"+formatter.format(name.getRen_date())+"')");
				}
				//Cancellation date
				if(name.getCancel_date()!=null){
					res=state.executeUpdate("INSERT INTO CancellationDates(name,cancelDate) "
							+ "VALUES('"+name.getBus_name()+"','"+formatter.format(name.getCancel_date())+"')");
				}
				//Former state number and state of registration
				if(name.getPre_state_reg()!=null||name.getForm_state_num()!=null){
					res=state.executeUpdate("INSERT INTO FormerStates(name,formerStateNum,previousRegState) "
							+ "VALUES('"+name.getBus_name()+"','"+name.getForm_state_num()+"','"+name.getPre_state_reg()+"')");
				}
			}
			//close statement
			state.close();
		}catch(Exception e){
			//System.err.println("Insertion into derby failed");
		}
	}
	
	/*Read in from derby database
	 * 
	 */
	public static long readDerby(String type){
		String derby="jdbc:derby:businessnames";
		int res;
		long start=-1,end=-1;
		
		try{
			//Establish connection
			Connection con= DriverManager.getConnection(derby);
			Statement state=con.createStatement();
			
			//Start timer
			start=System.currentTimeMillis();
			
			//Determine query type call
			//Serach database for records matching confdition (1 in this case)
			if(type.equalsIgnoreCase("equal")) {
				res=state.executeUpdate("SELECT * FROM BusinessNumbers WHERE name='';");
			}
			//Retrieve all records from single table
			else if(type.equalsIgnoreCase("all-single")){
				res=state.executeUpdate("SELECT * FROM BusinessNumbers;");
			}
			//Retrieve all records from all tables
			else if(type.equalsIgnoreCase("all-total")){
				res=state.executeUpdate("SELECT * FROM BusinessNumbers "
						+ "INNER JOIN RegistrationStatus ON BusinessNumbers.name = RegistrationStatus.name "
						+ "INNER JOIN RegistrationDates ON BusinessNumbers.name = RegistrationDates.name "
						+ "INNER JOIN RenewalDates ON BusinessNumbers.name = RenewalDates.name "
						+ "INNER JOIN CancellationDates ON BusinessNumbers.name = CancellationDates.name "
						+ "INNER JOIN FormerStates ON BusinessNumbers.name = FormerStates.name;");
			}else {
				System.err.println("Invalid type entered");
			}
			//Stop timer
			end=System.currentTimeMillis();
			
			//Close statement
			state.close();
		}catch(Exception e){
			System.err.println("Retrieval error encountered");
		}
		
		//Return query time taken
		return end-start;
	}
	
}
