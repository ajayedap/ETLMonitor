package com.aj.netapp;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class P0Missing {
	public static void main(String args[]){
		try{
		FileInputStream fstream = new FileInputStream("c:/textfile.txt");
		 DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  //Read File Line By Line
		  while ((strLine = br.readLine()) != null)   {
			  strLine=strLine.replaceAll("\\s","");			  
			  StringTokenizer stk=new StringTokenizer(strLine,",");
			  int i=0;
			  String date="";
			  while(stk.hasMoreElements()){
				  if(i==0)
				  date=stk.nextToken();
				  i++;
				  String hr=stk.nextToken();
				  String command="sh p0_manual_multiple.sh "+date+"/"+hr+" "+date+"/"+hr+" > /opt/pentaho/server/data-integration-server/logs/"+date+"_"+hr+".log &;wait";
				  System.out.println(command);	  
			  }
		  }
		  //Close the input stream
		  in.close();
		    }catch (Exception e){//Catch exception if any
		  System.err.println("Error: " + e.getMessage());
		  }
	}

}
