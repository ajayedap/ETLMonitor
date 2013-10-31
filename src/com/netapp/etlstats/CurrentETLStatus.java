package com.netapp.etlstats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

public class CurrentETLStatus {

	static final String WORK_DIR="/pentaho/home/ASUP_NEXT_DSS/bin/ETL_REPORT/";
	static final String CURRENT_STATUS_FILE = WORK_DIR+"files/current_status.txt";
	//static final String CURRENT_STATUS_FILE = "files/current_status.txt";
	private String cur_batch_id;
	private ArrayList<String> select_snap;
	private ArrayList<String> groupsCompleted = new ArrayList<String>();
	private ArrayList<String> groupsFailed = new ArrayList<String>();

	private int[] grpsCompltd = new int[7];
	private int[] grpsFailed = new int[7];

	CurrentETLStatus() throws Exception {
		// Always wrap FileReader in BufferedReader.
		BufferedReader bRead = new BufferedReader(new FileReader(
				CURRENT_STATUS_FILE));
		// set current batchid
		cur_batch_id = bRead.readLine();
		select_snap = new ArrayList<String>();
		String curLine = "";
		while ((curLine = bRead.readLine()) != null) {
			if (curLine.length() > 0)
				select_snap.add(curLine);
		}
		// intialize grpsCompltd to 0's
		for (int i = 0; i < grpsCompltd.length; i++) {
			grpsCompltd[i] = 0;
		}
		for (int i = 0; i < grpsFailed.length; i++) {
			grpsFailed[i] = -1;
		}
		if (select_snap.size() != 0) {
			findGroupsCompleted();
			findGroupsFailed();
		}

	}

	public String getCurrentBatchID() {
		return this.cur_batch_id;
	}

	public ArrayList<String> getSelectSnap() {
		return this.select_snap;
	}

	private void findGroupsCompleted() {
		String temp;
		for (int i = 0; i < select_snap.size(); i++) {
			temp = select_snap.get(i);// System.out.println(temp);
			String grp, phase, status;
			grp = temp.substring(0, 1);
			phase = temp.substring(2, 3);
			status = temp.substring(4);
			int grpN = Integer.parseInt(grp);
			if (status.equalsIgnoreCase("COMPLETED")) {
				grpsCompltd[grpN - 1]++;
			} else
				grpsCompltd[grpN - 1]=-9999;

		}

		if (grpsCompltd[0] == 4)
			groupsCompleted.add("1");
		if (grpsCompltd[1] == 1)
			groupsCompleted.add("2");
		if (grpsCompltd[2] == 1)
			groupsCompleted.add("3");
		if (grpsCompltd[3] == 2)
			groupsCompleted.add("4");
		if (grpsCompltd[4] == 1)
			groupsCompleted.add("5");
		if (grpsCompltd[5] == 1)
			groupsCompleted.add("6");
		if (grpsCompltd[6] == 1)
			groupsCompleted.add("7");

	}

	private void findGroupsFailed() {
		String temp;
		for (int i = 0; i < select_snap.size(); i++) {
			temp = select_snap.get(i);// System.out.println(temp);
			String grp, phase, status;
			grp = temp.substring(0, 1);
			phase = temp.substring(2, 3);
			status = temp.substring(4);
			int grpN = Integer.parseInt(grp);
			if (status.startsWith("Job can not trigger")
					|| status.startsWith("FAILED")) {
				grpsFailed[grpN - 1]++;
			}

		}
		if (grpsFailed[0] != -1)
			groupsFailed.add("1");
		if (grpsFailed[1] != -1)
			groupsFailed.add("2");
		if (grpsFailed[2] != -1)
			groupsFailed.add("3");
		if (grpsFailed[3] != -1)
			groupsFailed.add("4");
		if (grpsFailed[4] != -1)
			groupsFailed.add("5");
		if (grpsFailed[5] != -1)
			groupsFailed.add("6");
		if (grpsFailed[6] != -1)
			groupsFailed.add("7");

	}

	public ArrayList<String> groupsCompleted() {

		return this.groupsCompleted;
	}

	public ArrayList<String> groupsFailed() {

		return this.groupsFailed;
	}

	public int dataAvailability() {
		float numr = this.groupsCompleted().size();
		float data_avail = (numr / 7) * 100;
		//System.out.println("ArrayList size:"+this.groupsCompleted.size()+"\tData Availability="+data_avail);
		return (int) data_avail;
	}

	public String toString() {
		String theString = "";
//		for (int i = 0; i < this.select_snap.size(); i++) {
//			theString += "\n" + this.select_snap.get(i);
//		}
		theString=this.getCurrentBatchID()+"|"+this.groupsCompleted+"|"+this.groupsFailed();
		return theString;
	}

	public static void main(String[] args) throws Exception {
		CurrentETLStatus cs = new CurrentETLStatus();
		System.out.println("Groups Completed" + cs.groupsCompleted() + " = "
				+ cs.dataAvailability() + "%");
		System.out.println("Groups Failed" + cs.groupsFailed());
	}

}
