package database_con_ass2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class database_con_ass1 {

	public static void main(String[] args) {
		//Initialize
		ArrayList<Business_Name> list;
		Long start,end;
		
		//Set start timer
		start=System.currentTimeMillis();
		//Read CSV into program
		list=readCSV(args[0]);
		//Set end timer calculate and display time taken
		end=System.currentTimeMillis();
		System.out.println("Load CSV time"+(end-start));
		
		//Set start timer
		start=System.currentTimeMillis();
		//Insert into Dynamo
		insertDerby(list);

	}

	
	public static ArrayList<Business_Name> readCSV(String filename){
		String line="";
		String split="\\t";
		ArrayList<Business_Name> list = new ArrayList<Business_Name>();
		String bus_name,status;
		Date reg_date;
		Date cancel_date;
		Date ren_date;
		String form_state_num,pre_state_reg,abn;
		
		try (BufferedReader br = new BufferedReader(new FileReader(filename))){
			while((line=br.readLine())!=null) {
				String[] hold = line.split(split);
				
				pre_state_reg="";
				abn="";
				
				bus_name=hold[1];
				status=hold[2];
				if(hold.length>6) {
					form_state_num=hold[6];
					if(hold.length>7) {
						pre_state_reg=hold[7];
						if(hold.length>8) {
							abn=hold[8];
						}else {
							abn="";
						}
					}else {
						pre_state_reg="";
					}
				}else {
					form_state_num="";
				}
				if(hold[3].isEmpty()) {
					reg_date=null;
				}else {
					reg_date=new SimpleDateFormat("dd/MM/yyyy").parse(hold[3]);
				}
				if(hold[4].isEmpty()) {
					cancel_date=null;
				}else {
					cancel_date=new SimpleDateFormat("dd/MM/yyyy").parse(hold[4]);
				}
				if(hold[5].isEmpty()) {
					ren_date=null;
				}else {
					ren_date=new SimpleDateFormat("dd/MM/yyyy").parse(hold[5]);
				}
				
				list.add(new Business_Name(bus_name,status,reg_date,cancel_date,ren_date,form_state_num,pre_state_reg,abn));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static void insertDerby(ArrayList<Business_Name> list){
		String derby="jdbc:derby:businessnames";
		
		try{
			Connection con= DriverManager.getConnection(derby);
			Statement state=con.createStatement();
			for(Business_Name name: list){
				int res=state.executeUpdate("INSERT INTO names() VALUES("+name+")");
			}
			state.close();
		}catch(Exception e){
			
		}
	}
}
