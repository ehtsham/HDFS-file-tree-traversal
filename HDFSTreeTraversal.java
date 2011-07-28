package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;


public class HDFSTreeTraversal {
	public static void fileTreeCreation() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = null;
		FSDataOutputStream out = null;
		String s = null;
		BufferedReader keyboardin = new BufferedReader(
				(new InputStreamReader(System.in)));
		String q = " ";
		String wuri;
		try{
			while(!q.equals("q")){
				System.out.println("Enter the uri");
				wuri = keyboardin.readLine();
				try{
					System.out.println("Write a story");
					fs=FileSystem.get(URI.create(wuri), conf);
					out = fs.create(new Path(wuri));
					while(!(s = keyboardin.readLine()).equals("-1")){
						out.write(s.getBytes("UTF-8"));
					}
				}finally{
					IOUtils.closeStream(out);
					System.out.format("Story written to %s%n",wuri);
				}
				System.out.println("Do you want to exit: q");
				q=keyboardin.readLine();
			}
		}finally{
			keyboardin.close();
		}
	}
	
	public static void fileTreeRecursion(String uri) throws IOException{
		//Because, file structure is a tree, so no object creation is required,
		// hence my work is easier
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		fileTreeRecursion(URI.create(uri),conf,fs);
	
	}
	public static void fileTreeRecursion(URI uri,Configuration conf,FileSystem fs) throws IOException{
		Path current = new Path(uri);
		if(fs.isFile(current)){
			visit(current,fs);
		}else{
			FileStatus[] status = fs.listStatus(current);
		    Path[] paths = FileUtil.stat2Paths(status);
		    for(Path p: paths){
		    	fileTreeRecursion(p.toUri(),conf,fs);	
		    }
		}
	}
	public static void visit(Path p, FileSystem fs){
		FSDataInputStream in = null;
		try{
			in = fs.open(p);
			System.out.println("Printing story from "+p.toString());
			IOUtils.copyBytes(in, System.out, 4096, false);
		}catch(IOException e){
			System.err.println("Reading error");
			System.exit(1);
		}
		finally{
			IOUtils.closeStream(in);
			System.out.println("\n");
		}
	}
	public static void main(String args[]){
		try{
			//fileTreeCreation();
			fileTreeRecursion("hdfs://localhost/user/munawar/hdfsio");
		}catch(IOException e){
			System.err.println("Something went wrong");
		}
	}
}
