package test;

import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Test extends ToolJob{

	public static void main (String[] args) {
		try {
			Configuration conf = new Configuration();
			ToolRunner.run(conf, new Test(), args);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		return 1;
	}
		
	
}
