

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class database_con_ass1 {

	public static void main(String[] args) {
		//Initialize
		Long start,end;
		
		//Set start timer
		start=System.currentTimeMillis();
		//Read CSV into program
		readCSV(args[0]);
		//Set end timer calculate and display time taken
		end=System.currentTimeMillis();
		System.out.println("Load CSV time"+(end-start));
		
	}

	
	public static void readCSV(String filename){
		String line="";
		ArrayList<Record> list = new ArrayList<Record>();
		
		//Read in CSV file
		try (BufferedReader br = new BufferedReader(new FileReader(filename))){
			
			br.readLine();
			//Read in each line
			while((line=br.readLine())!=null) {
				//Add Business_name object to list read in
				list.add(generateRecord(line));
				if(list.size()>9){
					DerbyMethods.insertDerby(list);
					list.clear();
					System.gc();
				}
			}
		}catch(Exception e) {
			//System.err.println("CSV file read failed");
			//e.printStackTrace();
		}
		
	}
	
	/*Generate record from provided csv string
	 * @param line CSV line of record
	 * @return Record object representing data from csv
	 */
	public static Record generateRecord(String line) throws Exception{
		String bus_name,status;
		Date reg_date;
		Date cancel_date;
		Date ren_date;
		String form_state_num,pre_state_reg,abn;
		Record rec;
		String split="\\t";
				
		//Set null values for potentially unreachable values
		pre_state_reg=null;
		abn=null;
		form_state_num=null;
				
		//Split line on delim
		String[] hold = line.split(split);
				
		//Set values known to be p[resent
		bus_name=hold[1];
		status=hold[2];
				
		//Determine if record is long enough to contain values and set if so
		if(hold.length>6) {
			form_state_num=hold[6];
			if(hold.length>7) {
				pre_state_reg=hold[7];
				if(hold.length>8) {
					abn=hold[8];
				}
			}
		}
				
		//Null dates in no entry else convert string to date value
		//Registration date
		if(hold[3].isEmpty()) {
			reg_date=null;
		}else {
			reg_date=new SimpleDateFormat("dd/MM/yyyy").parse(hold[3]);
		}
		//Cancellation date
		if(hold[4].isEmpty()) {
			cancel_date=null;
		}else {
			cancel_date=new SimpleDateFormat("dd/MM/yyyy").parse(hold[4]);
		}
		//Renewal date
		if(hold[5].isEmpty()) {
			ren_date=null;
		}else {
			ren_date=new SimpleDateFormat("dd/MM/yyyy").parse(hold[5]);
		}
				
		//Add Business_name object to list read in
		rec=new Record(bus_name,status,reg_date,cancel_date,
				ren_date,form_state_num,pre_state_reg,abn);
		return rec;
	}
	
	
}
