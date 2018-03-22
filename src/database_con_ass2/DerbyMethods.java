package database_con_ass2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;

public class DerbyMethods {
	public static void insertDerby(ArrayList<Business_Name> list){
		String derby="jdbc:derby:businessnames";
		
		try{
			Connection con= DriverManager.getConnection(derby);
			Statement state=con.createStatement();
			for(Business_Name name: list){
				int res;
				if(name.getAbn()!=null){
					res=state.executeUpdate("INSERT INTO BusinessNumbers(name,abn) "
							+ "VALUES("+name.getBus_name()+","+name.getAbn()+");");
				}
				if(name.getStatus()!=null){
					res=state.executeUpdate("INSERT INTO RegistrationStatus(name,status) "
							+ "VALUES("+name.getBus_name()+","+name.getStatus()+");");
				}
				if(name.getReg_date()!=null){
					res=state.executeUpdate("INSERT INTO RegistrationDates(name,registrationDate) "
							+ "VALUES("+name.getBus_name()+","+name.getReg_date()+");");
				}
				if(name.getRen_date()!=null){
					res=state.executeUpdate("INSERT INTO RenewalDates(name,renewalDate) "
							+ "VALUES("+name.getBus_name()+","+name.getRen_date()+");");
				}
				if(name.getCancel_date()!=null){
					res=state.executeUpdate("INSERT INTO CancellationDates(name,cancelDate) "
							+ "VALUES("+name.getBus_name()+","+name.getCancel_date()+");");
				}
				if(name.getPre_state_reg()!=null||name.getForm_state_num()!=null){
					res=state.executeUpdate("INSERT INTO FormerStates(name,formerStateNum,PreviousRegState) "
							+ "VALUES("+name.getBus_name()+","+name.getForm_state_num()+","+name.getPre_state_reg()+");");
				}
			}
			state.close();
		}catch(Exception e){
			
		}
	}
	
	public static long readDerby(String type){
		String derby="jdbc:derby:businessnames";
		int res;
		long start=-1,end=-1;
		
		try{
			Connection con= DriverManager.getConnection(derby);
			Statement state=con.createStatement();
				start=System.currentTimeMillis();
				if(type.equalsIgnoreCase("equal")) {
					res=state.executeUpdate("SELECT * FROM BusinessNumbers WHERE name='';");
				}else if(type.equalsIgnoreCase("all-single")){
					res=state.executeUpdate("SELECT * FROM BusinessNumbers;");
				}else if(type.equalsIgnoreCase("all-total")){
					res=state.executeUpdate("SELECT * FROM BusinessNumbers "
							+ "INNER JOIN RegistrationStatus ON BusinessNumbers.name = RegistrationStatus.name "
							+ "INNER JOIN RegistrationDates ON BusinessNumbers.name = RegistrationDates.name "
							+ "INNER JOIN RenewalDates ON BusinessNumbers.name = RenewalDates.name "
							+ "INNER JOIN CancellationDates ON BusinessNumbers.name = CancellationDates.name "
							+ "INNER JOIN FormerStates ON BusinessNumbers.name = FormerStates.name;");
				}else {
					System.err.println("Invalid type entered");
				}
				end=System.currentTimeMillis();
			state.close();
		}catch(Exception e){
			System.err.println("Retrieval error encountered");
		}
		return end-start;
	}
	
}
