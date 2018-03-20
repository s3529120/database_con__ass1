package database_con_ass2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class database_con_ass1 {

	public static void main(String[] args) {
		ArrayList<Business_Name> list;
		if(args.length<1) {
			list=readCSV(args[1]);
		}

	}

	
	public static ArrayList<Business_Name> readCSV(String filename){
		String line="";
		String split=",";
		ArrayList<Business_Name> list = new ArrayList<Business_Name>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(filename))){
			while((line=br.readLine())!=null) {
				String[] hold = line.split(split);
				list.add(new Business_Name(hold[0],hold[1],new SimpleDateFormat("dd/MM/yyyy").parse(hold[2]),
						new SimpleDateFormat("dd/MM/yyyy").parse(hold[3]),new SimpleDateFormat("dd/MM/yyyy").parse(hold[4]),hold[5],hold[6],hold[7]));
			}
		}catch(Exception e) {
			
		}
		return list;
	}
}
