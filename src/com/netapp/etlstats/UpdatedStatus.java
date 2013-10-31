package com.netapp.etlstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

public class UpdatedStatus extends PreviousStatus {

//
//	public UpdatedStatus(CurrentETLStatus current) {
//		super("dummy");
//		//System.out.println("In subclass constructor:");
//		this.batch_id=current.getCurrentBatchID();
//		this.grpsCompltd=current.groupsCompleted();
//		this.grpsFailed=current.groupsFailed();
//	}
	public UpdatedStatus(){
		
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

	public void updateLatestStatus() {

	}

	public String toString() {
		String theRecord;
		theRecord = this.getBatchId() + "|";
//		theRecord += this.getStartTime().getTime() + "|";
//		theRecord += this.getAsupDateRange() + "|";
//		theRecord += this.getNoOfAsups() + "|";
//		theRecord += this.getETA().getTime() + "|";
		theRecord += this.getGroupsCompleted() + "|";
		theRecord += this.getGroupsFailed() + "|";
//		theRecord += this.getComments() + "|";
//		theRecord += this.getStatus() + "|";
//		theRecord += this.getTouch().getTime();
		return theRecord;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
