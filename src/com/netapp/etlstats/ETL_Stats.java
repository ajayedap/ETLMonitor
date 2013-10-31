package com.netapp.etlstats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class ETL_Stats {
	int batch_id;
	String batch_start_at;
	String asup_date_range;
	int no_of_asups;
	String ETA;
	int data_avail;
	String batch_cur_status;
	String sla;
	String batch_run_time;
	String comments;
	
	int cur_batch_id;
	String cur_overallStatus;
	Date temp_start_date;
	
	public void loadCurrentData() throws IOException, ParseException{
		// FileReader reads text files in the default encoding.
		FileReader fileReader=new FileReader("current_data.txt");
		
		// Always wrap FileReader in BufferedReader.
        BufferedReader bRead =   new BufferedReader(fileReader);
        String overAll=bRead.readLine();
        String[] tokens=overAll.split("\t");
        cur_batch_id=Integer.parseInt(tokens[0]);
        //merge field
        if(tokens[2].equals("STARTED"))
        	setCurrentStatus("MERGING");
        //overall status
        if(tokens[3].equals("COMPLETED"))
        	cur_overallStatus="COMPLETED";
        else cur_overallStatus="IN-PROGRESS";
        //batch start at
        setStartTime(tokens[4]);
        
	}
	private void setCurrentStatus(String string) {
		// TODO Auto-generated method stub
		
	}
	public void setStartTime(String fromQuery) throws ParseException{
		
		String dd,mn,yy,hh,mm,ap;
		StringTokenizer stk=new StringTokenizer(fromQuery,"-. ");
//		while(stk.hasMoreElements())
//			System.out.println(stk.nextToken());
		dd=stk.nextToken();
		mn=stk.nextToken();
		yy=stk.nextToken();
		hh=stk.nextToken();
		mm=stk.nextToken();
		stk.nextToken();stk.nextToken();
		ap=stk.nextToken();
		if(ap.equals("PM"))
			hh=String.valueOf(12+Integer.parseInt(hh));
		//SimpleDateFormat sdf=new SimpleDateFormat("dd MMM yyyy HH:mm z");
		SimpleDateFormat sdf=new SimpleDateFormat("dd MMM yyyy HH:mm z");
		
		String allDateString=dd+" "+mn+" 20"+yy+" "+hh+":"+mm+" PST";
		System.out.println(allDateString);
		temp_start_date=sdf.parse(allDateString);
		System.out.println(temp_start_date);
	}

}
