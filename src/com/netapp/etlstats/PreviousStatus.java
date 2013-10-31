package com.netapp.etlstats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

public class PreviousStatus {

	static final String WORK_DIR="/pentaho/home/ASUP_NEXT_DSS/bin/ETL_REPORT/";
	static final String PREVIOUS_STATUS_FILE = WORK_DIR+"files/previous_status.txt";
	//static final String PREVIOUS_STATUS_FILE = "files/previous_status.txt";
	String batch_id;
	Date start_time;
	String asup_date_range;
	int no_of_asups;
	Date eta;
	ArrayList<String> grpsCompltd;
	ArrayList<String> grpsFailed;
	StringBuilder comments;
	String status;
	Date touch;

	public PreviousStatus(String dummy) {
		// System.out.println("In super class dummy constructor");
	}

	PreviousStatus() {
		try {
			// System.out.println("In super class actual constructor");
			BufferedReader bRead = new BufferedReader(new FileReader(
					PREVIOUS_STATUS_FILE));
			String lastLine = null, tempLine = "";
			while ((tempLine = bRead.readLine()) != null) {
				if (tempLine.length() > 9)
					lastLine = tempLine;
			}
			bRead.close();// close the file as reading is done

			String[] tokens = lastLine.split("\\|");
			batch_id = tokens[0];
			start_time = new Date(Long.parseLong(tokens[1])*1000);
			asup_date_range = tokens[2];
			no_of_asups = Integer.parseInt(tokens[3]);
			eta = new Date(Long.parseLong(tokens[4])*1000);
			grpsCompltd = new ArrayList<String>();
			String[] compltdGrps = tokens[5].split(",");
			for (String compltdGrp : compltdGrps)
				grpsCompltd.add(compltdGrp);
			grpsFailed = new ArrayList<String>();
			String[] failedGrps = tokens[6].split(",");
			for (String failedGrp : failedGrps)
				grpsFailed.add(failedGrp);
			comments = new StringBuilder(tokens[7]);
			status = tokens[8];
			touch = new Date(Long.parseLong(tokens[9])*1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getBatchId() {
		return this.batch_id;
	}

	public Date getStartTime() {
		return this.start_time;
	}

	public String getAsupDateRange() {
		return this.asup_date_range;
	}

	public int getNoOfAsups() {
		return this.no_of_asups;
	}

	public Date getETA() {
		return this.eta;
	}

	public ArrayList<String> getGroupsCompleted() {
		return this.grpsCompltd;
	}

	public ArrayList<String> getGroupsFailed() {
		return this.grpsFailed;
	}

	public void addToCompleted(String group) {
		this.grpsCompltd.add(group);
	}

	public void addToFailed(String group) {
		this.grpsFailed.add(group);
	}

	public StringBuilder getComments() {
		return this.comments;
	}

	public String getStatus() {
		return this.status;
	}

	public Date getTouch() {
		return this.touch;
	}

	public int dataAvailability() {
		float numr = this.grpsCompltd.size();
		float data_avail = (numr / 7) * 100;
		return (int) data_avail;
	}

	public String toString() {
		// StringBuilder toReturn = new StringBuilder();
		// toReturn.append("BatchID:" + batch_id + "\n");
		// toReturn.append("Start Time:" + start_time + "\n");
		// toReturn.append("asup_date_range:"+asup_date_range+"\n");
		// toReturn.append("No.Of Asups# "+no_of_asups+"\n");
		// toReturn.append("ETA:"+eta+"\n");
		// toReturn.append("Groups Completed:"+grpsCompltd+"\n");
		// toReturn.append("Groups Failed"+grpsFailed+"\n");
		// toReturn.append("Comments:"+comments+"\n");
		// toReturn.append("Status:"+status+"\n");
		// toReturn.append("Touch:"+touch);

		// return toReturn.toString();
		String grpsCompleted = "", grpsFailed = "";
		// System.out.println("In toString of PreviousStatus"+this.getGroupsCompleted());
		for (String aGroup : this.getGroupsCompleted()) {
			grpsCompleted += aGroup + ",";
			// System.out.println(grpsCompleted);
		}
		
		if(grpsCompleted.equals(","))
			grpsCompleted="";
		else if(grpsCompleted.startsWith(","))
			grpsCompleted = grpsCompleted.substring(1,
					grpsCompleted.length() - 1);
		if(grpsCompleted.endsWith(","))
			grpsCompleted=grpsCompleted.substring(0, grpsCompleted.length()-1);

		for (String aGroup : this.getGroupsFailed()) {
			grpsFailed += aGroup + ",";
		}

		if(grpsFailed.equals(","))
			grpsFailed="";
		else if(grpsFailed.startsWith(","))
			grpsFailed = grpsFailed.substring(1,
					grpsFailed.length() - 1);
		if(grpsFailed.endsWith(","))
			grpsFailed=grpsFailed.substring(0, grpsFailed.length()-1);
		long start_in_sec=this.start_time.getTime()/1000;
		long eta_in_sec=this.getETA().getTime()/1000;
		long touch_in_sec=touch.getTime()/1000;
		return batch_id + "|" + start_in_sec + "|" + asup_date_range
				+ "|" + no_of_asups + "|" + eta_in_sec + "|" + grpsCompleted
				+ "|" + grpsFailed + "|" + comments + "|" + status + "|"
				+ touch_in_sec;
	}

	public void setGroupsCompleted(ArrayList<String> _grps_comp) {
		this.grpsCompltd = _grps_comp;
	}

	public void setGroupsFailed(ArrayList<String> _grps_fld) {
		this.grpsFailed = _grps_fld;
	}

	public void setComments(StringBuilder _cmnts) {
		this.comments = _cmnts;
	}

	public void setStatus(String _status) {
		this.status = _status;
	}

	public void setTouch(Date _tch) {
		this.touch = _tch;
	}

	public static void main(String[] args) throws IOException {
		PreviousStatus prev = new PreviousStatus();
		System.out.println(prev);

	}

}
