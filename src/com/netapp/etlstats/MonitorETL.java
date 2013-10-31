package com.netapp.etlstats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.*;

public class MonitorETL {
	public static String ENV;
	public static String MAIL_FROM;
	public static String MAIL_TO;
	private static boolean mailable = false;
	public CurrentETLStatus current;
	public PreviousStatus previous;

	public static enum TheContext {
		NO_BATCH_SINCE_12_HRS, BATCH_STARTED, MERGED, GROUP_COMPLETED, GROUP_FAILED, BATCH_COMPLETED
	}

	public MonitorETL() throws Exception {

		current = new CurrentETLStatus();// object which holds
											// current ETL
											// status i,e.,
											// loads from
											// current_status.txt
		System.out.println("Current:" + current);
		previous = new PreviousStatus();// Object which holds
										// latest/previous
										// status i,e., from
										// previous_status.txt
		System.out.println("Previous:" + previous);

		if (current.getCurrentBatchID().equals(previous.getBatchId())
				&& previous.getStatus().equals("COMPLETED")) {
			System.out.println("No batch running.");
			long minutes = new Date().getTime()
					- previous.getStartTime().getTime() / 1000 / 60 / 60;
			// if (minutes >= 12)
			// alertRequired(TheContext.NO_BATCH_SINCE_12_HRS);
		} else if (previous.getStatus().equals("NEW")) {
			System.out.println("We have a brand new batch.");
			alertRequired(TheContext.BATCH_STARTED);
		} else if (previous.getStatus().equals("MERGED")) {
			System.out.println("Merging just got completed.");
			// TODO:append to previous_status.txt as
			// $CUR_BATCH_ID||||||||INPROGRESS|touch
			alertRequired(TheContext.MERGED);
		} else if (previous.getStatus().equals("INPROGRESS")) {
			System.out.print("Groups have begun and we have "
					+ current.dataAvailability()
					+ "% of data availability with ");
			int gpsFailedN = current.groupsFailed().size();
			System.out.println(gpsFailedN + " groups failed.");
			if (gpsFailedN == 0)
				previous.setGroupsFailed(current.groupsFailed());
			// here goes the running groups
			if (current.dataAvailability() == 100) {
				if (previous.dataAvailability() != 100) {
					System.out.println("Batch just got completed");
					previous.setGroupsCompleted(current.groupsCompleted());
				}
				alertRequired(TheContext.BATCH_COMPLETED);
			} else {
				if (current.groupsCompleted().size() > previous
						.getGroupsCompleted().size()) {
					System.out.println("There is group newly completed.");
					alertRequired(TheContext.GROUP_COMPLETED);
				}
				// now check for failed groups
				for (String inCurrent : current.groupsFailed()) {
					if (!previous.grpsFailed.contains(inCurrent)) {
						System.out.println("There is a group newly failed.");
						alertRequired(TheContext.GROUP_FAILED);
					}
				}
			}

		}

	}

	public void alertRequired(TheContext context) throws Exception {
		String subject;
		String body;
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm");
		subject = ENV + ":ETL: " + previous.getBatchId() + ":";
		switch (context) {
		case NO_BATCH_SINCE_12_HRS:
			subject = ENV + ".::ETL::.";
			subject += "Is ETL paused ?";
			body = "No new batch has been identified past 12 hours.";
			sendMail(subject, body);
			break;
		case BATCH_STARTED:
			subject += "New batch started";
			body = "<b>A new batch started at " + previous.getStartTime()
					+ " with an asup threshold of " + previous.getComments()
					+ "</b>";
			// System.out.println("A mail sent as --"+subject+"-- and contains --"+body+"--");
			previous.setStatus("MERGING");
			// Date date=new Date();
			System.out.println("Updated:" + previous);
			sendMail(subject, body);
			updateStatusToFile(previous);
			break;
		case MERGED:
			previous.setStatus("INPROGRESS");
			subject += "Merging Completed";
			body = "<b>Merging just got completed</b><br><br><br>";
			body += "<table border=\"1\"><tr><th style=\"background-color:#006699\">Batch ID</th><th style=\"background-color:#006699\">Batch Start Date/Hr (PDT)</th><th style=\"background-color:#006699\">ASUP Date Range (PDT)</th><th style=\"background-color:#006699\">ASUPs</th><th style=\"background-color:#006699\">ETA/Batch End Date (PDT)</th><th style=\"background-color:#006699\">Data Availability %</th><th style=\"background-color:#006699\">Batch Status</th><th style=\"background-color:#006699\">Within SLA (Yes/No)</th><th style=\"background-color:#006699\">Batch Total Runtime</th></tr>\n";
			body += "</tr><tr><td>" + previous.getBatchId() + "</td><td>"
					+ format.format(previous.getStartTime()) + "</td><td>"
					+ previous.getAsupDateRange() + "</td><td>"
					+ previous.getNoOfAsups() + "</td><td>"
					+ format.format(previous.getETA()) + "</td><td>"
					+ current.dataAvailability() + "</td><td>"
					+ previous.getStatus() + "</td><td>" + "XX" + "</td><td>"
					+ runTime() + "(HH:MM) </table>";
			sendMail(subject, body);
			updateStatusToFile(previous);
			break;
		case GROUP_COMPLETED:
			String[] allGroups = { "1", "2", "3", "4", "5", "6", "7" };
			subject = ENV + ":ETL: " + previous.getBatchId() + ":";
			for (String aGroup : allGroups) {
				if (current.groupsCompleted().contains(aGroup)
						&& !previous.getGroupsCompleted().contains(aGroup)) {
					previous.addToCompleted(aGroup);
					subject += "Group " + aGroup + " completed - ";
					body = "<b>Groups Completed:" + previous.grpsCompltd
							+ "\n</b><br>";
					body += "<b>Groups Failed:" + previous.grpsFailed
							+ "</b><br><br><br>";
					body += "<table border=\"1\"><tr><th style=\"background-color:#006699\">Batch ID</th><th style=\"background-color:#006699\">Batch Start Date/Hr (PDT)</th><th style=\"background-color:#006699\">ASUP Date Range (PDT)</th><th style=\"background-color:#006699\">ASUPs</th><th style=\"background-color:#006699\">ETA/Batch End Date (PDT)</th><th style=\"background-color:#006699\">Data Availability %</th><th style=\"background-color:#006699\">Batch Status</th><th style=\"background-color:#006699\">Within SLA (Yes/No)</th><th style=\"background-color:#006699\">Batch Total Runtime</th></tr>\n";
					body += "</tr><tr><td>" + previous.getBatchId()
							+ "</td><td>"
							+ format.format(previous.getStartTime())
							+ "</td><td>" + previous.getAsupDateRange()
							+ "</td><td>" + previous.getNoOfAsups()
							+ "</td><td>" + format.format(previous.getETA())
							+ "</td><td>" + current.dataAvailability()
							+ "</td><td>" + previous.getStatus() + "</td><td>"
							+ "XX" + "</td><td>" + runTime() + "(HH:MM) </table>";
					sendMail(subject, body);
					updateStatusToFile(previous);
					subject = ENV + ":ETL: " + previous.getBatchId() + ":";
				}
			}
			break;
		case GROUP_FAILED:
			String[] allGroupsF = { "1", "2", "3", "4", "5", "6", "7" };
			subject = ENV + ":ETL: " + previous.getBatchId() + ":";
			for (String aGroup : allGroupsF) {
				if (current.groupsFailed().contains(aGroup)
						&& !previous.getGroupsFailed().contains(aGroup)) {
					previous.addToFailed(aGroup);
					subject += "Group " + aGroup + " Failed";
					body = "<b>Groups Completed:" + previous.grpsCompltd
							+ "\n</b><br>";
					body += "<b>Groups Failed:" + previous.grpsFailed
							+ "</b><br>";
					body += "<b>There is a job in FAILED or JOB CAN't TRIGGER state in Group "
							+ aGroup + ".</b><br><br><br>";
					body += "<table border=\"1\"><tr><th style=\"background-color:#006699\">Batch ID</th><th style=\"background-color:#006699\">Batch Start Date/Hr (PDT)</th><th style=\"background-color:#006699\">ASUP Date Range (PDT)</th><th style=\"background-color:#006699\">ASUPs</th><th style=\"background-color:#006699\">ETA/Batch End Date (PDT)</th><th style=\"background-color:#006699\">Data Availability %</th><th style=\"background-color:#006699\">Batch Status</th><th style=\"background-color:#006699\">Within SLA (Yes/No)</th><th style=\"background-color:#006699\">Batch Total Runtime</th></tr>\n";
					body += "</tr><tr><td>" + previous.getBatchId()
							+ "</td><td>"
							+ format.format(previous.getStartTime())
							+ "</td><td>" + previous.getAsupDateRange()
							+ "</td><td>" + previous.getNoOfAsups()
							+ "</td><td>" + format.format(previous.getETA())
							+ "</td><td>" + current.dataAvailability()
							+ "</td><td>" + previous.getStatus() + "</td><td>"
							+ "XX" + "</td><td>" + runTime() + "(HH:MM) </table>";
					sendMail(subject, body);
					updateStatusToFile(previous);
					subject = ENV + ":ETL: " + previous.getBatchId() + ":";
				}
			}
			break;
		case BATCH_COMPLETED:
			previous.setStatus("COMPLETED");
			subject += "Batch completed";
			body = "<b>Batch Completed.</b><br><br><br>";
			body += "<table border=\"1\"><tr><th style=\"background-color:#006699\">Batch ID</th><th style=\"background-color:#006699\">Batch Start Date/Hr (PDT)</th><th style=\"background-color:#006699\">ASUP Date Range (PDT)</th><th style=\"background-color:#006699\">ASUPs</th><th style=\"background-color:#006699\">ETA/Batch End Date (PDT)</th><th style=\"background-color:#006699\">Data Availability %</th><th style=\"background-color:#006699\">Batch Status</th><th style=\"background-color:#006699\">Within SLA (Yes/No)</th><th style=\"background-color:#006699\">Batch Total Runtime</th></tr>\n";
			body += "</tr><tr><td>" + previous.getBatchId() + "</td><td>"
					+ format.format(previous.getStartTime()) + "</td><td>"
					+ previous.getAsupDateRange() + "</td><td>"
					+ previous.getNoOfAsups() + "</td><td>"
					+ format.format(previous.getETA()) + "</td><td>"
					+ current.dataAvailability() + "</td><td>"
					+ previous.getStatus() + "</td><td>" + "XX" + "</td><td>"
					+ runTime() + "(HH:MM) </table>";
			sendMail(subject, body);
			updateStatusToFile(previous);
			break;
		}

	}

	public String runTime() {

		int secsIn = (int) (new Date().getTime() / 1000 - previous
				.getStartTime().getTime() / 1000);
		int hours = secsIn / 3600, remainder = secsIn % 3600, minutes = remainder / 60;

		return ((hours < 10 ? "0" : "") + hours + ":"
				+ (minutes < 10 ? "0" : "") + minutes);

	}

	public void updateStatusToFile(PreviousStatus updated) throws Exception {
		updated.setGroupsFailed(current.groupsFailed());
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
				updated.PREVIOUS_STATUS_FILE, true)));
		updated.setTouch(new Date());
		out.println(updated);
		System.out.println("Status updated to the file");
		out.close();
	}

	public static void sendMail(String subject, String body)
			throws IOException, InterruptedException {
		System.out.println("Mail:\n=======================");
		System.out.println("Subject:" + subject);
		System.out.println("Body:" + body);
		mailable = true;
		StringBuilder theMail = new StringBuilder();
		theMail.append("FROM: " + MAIL_FROM + "\n");
		// theMail.append("To: ajaye@netapp.com\n");
		theMail.append("MIME-Version: 1.0\nContent-Type: text/html\n");
		theMail.append("Subject: " + subject + "\n");
		theMail.append("<html>");
		theMail.append(body);
		theMail.append("</html>\n");
		System.out.println("--------------\n" + theMail.toString());
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
				"mail.msg")));
		out.println(theMail.toString());
		out.close();
		Process p = Runtime.getRuntime().exec("/usr/sbin/sendmail " + MAIL_TO);
		OutputStream os = p.getOutputStream();
		InputStream is = new FileInputStream("mail.msg");
		// copy the contents
		byte[] buf = new byte[4096];
		int len;
		while ((len = is.read(buf)) != -1) {
			os.write(buf, 0, len); // only write as many as you have read
		}
		os.close();
		is.close();
		System.out.println("Mail Sent.");
	}

	public static void sendErrorMail(String subject, String body)
			throws IOException, InterruptedException {
		mailable = true;
		StringBuilder theMail = new StringBuilder();
		theMail.append("FROM: " + MAIL_FROM + "\n");
		// theMail.append("To: ajaye@netapp.com\n");
		theMail.append("MIME-Version: 1.0\nContent-Type: text/html\n");
		theMail.append("Subject: " + subject + "\n");
		theMail.append("<html>");
		theMail.append(body);
		theMail.append("</html>\n");
		System.out.println("--------------\n" + theMail.toString());
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
				"mail.msg")));
		out.println(theMail.toString());
		out.close();
		
		Process p = Runtime.getRuntime().exec(
				"/usr/sbin/sendmail ajaye@netapp.com");
		OutputStream os = p.getOutputStream();
		InputStream is = new FileInputStream("mail.msg");
		// copy the contents
		byte[] buf = new byte[4096];
		int len;
		while ((len = is.read(buf)) != -1) {
			os.write(buf, 0, len); // only write as many as you have read
		}
		os.close();
		is.close();
		
	}

	public static void main(String args[]) {
		if (args.length == 2) {
			ENV = args[0];
			if (ENV.equalsIgnoreCase("STG"))
				MAIL_FROM = "etl_monitor_stage@netapp.com";
			else if (ENV.equalsIgnoreCase("PRD"))
				MAIL_FROM = "etl_monitor_prod@netapp.com";
			MAIL_TO = args[1];

		}

		System.out.println("-----------------------\n");
		System.out.println(new Date());

		MonitorETL etl = null;
		try {
			etl = new MonitorETL();
		} catch (Exception e) {
			// TODO Auto-generated catch block

			try {
				String error = e.toString();
				System.out.println("This exception will be reported:" + error);
				sendErrorMail(ENV + ":ETL Monitor:Exception", error);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

	}

}
