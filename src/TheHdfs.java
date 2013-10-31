import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class TheHdfs {

	Path thePath;
	Configuration conf;
	FileSystem fs;

	public TheHdfs() throws Exception {
		conf = new Configuration();
		//conf.set("fs.default.name","");
		thePath = new Path("/etl/archieve_p0/*/*/p0-threshold/*");
		for (FileStatus s : fs.listStatus(thePath)) {
			System.out.println(s.getPath());
		}
	}

	public static void main(String args[]) throws Exception {
		System.out.println("Main Entered");
		new TheHdfs();
	}
}
