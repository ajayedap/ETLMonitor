package com.netapp.etlstats;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Temp {

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		System.out.println(new Date().getTime());
		int temp=(int) (new Date().getTime()/1000);
		System.out.println(temp);
	}

}
