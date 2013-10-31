package com.netapp.p0missing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class P0MissingBuckets {

	Path thePath;
	Configuration conf;
	FileSystem fs;
	
	String start_date;
	String end_date;

	public P0MissingBuckets() throws IOException {
		conf = new Configuration();
		// conf.addResource(new Path("/home/chinna/hadoop/conf/core-site.xml"));
		// conf.addResource(new Path("/home/chinna/hadoop/conf/hdfs-site.xml"));
		// conf.addResource(new
		// Path("/home/chinna/hadoop/conf/mapred-site.xml"));
		conf.set("fs.default.name", "hdfs://localhost:54310/");
		fs = FileSystem.get(conf);

	}

	public void getListing(String _path) throws IOException {

		this.thePath = new Path(_path);
		for (FileStatus s : fs.listStatus(thePath)) {
			System.out.println("Name:" + s.getPath() + " is a "
					+ (s.isDir() ? " Directory." : " File."));
			if (s.isDir())
				getListing(s.getPath().toString());
			// System.out.println("Total size:"+s.getLen());
			// System.out.println("Block Size:"+s.getBlockSize());
			// System.out.println("File Permissions:"+s.getPermission());
		}

	}

	public void traversePath(String _path) throws IOException {
		this.thePath = new Path(_path);
		for (FileStatus s : fs.listStatus(thePath)) {
			System.out.println("Name:" + s.getPath() + " is a "
					+ (s.isDir() ? " Directory." : " File."));
			if (s.isDir())
				traversePath(s.getPath().toString()); // goes recursive here
			else {
				// getNumberofBlocks(s.getPath().toString());
			}
		}

	}

	
	public static void main(String args[]) throws IOException {
		long startTime = System.nanoTime();
		if(!args[0].matches("....\\-..\\-..") || !args[1].matches("....\\-..\\-..")){
			System.out.println("Wrong input!\nStart and end dates must be of format YYYY-MM-DD");
			System.exit(1);
		}
		else System.out.println("Input matched");
//		P0MissingBuckets fls = new P0MissingBuckets();
	//	 fls.traversePath("india");
		long endTime = System.nanoTime();	
		
		System.out.println("\n\nTime Taken: "+(endTime - startTime)/1000000000.0 + " secs"); 
	}

}