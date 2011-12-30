package test;

import org.apache.hadoop.util.ToolRunner;

import lib.ToolJob;

public class Test extends ToolJob{

	public static void main (String[] args) {
		try {
			ToolRunner.run(new Test(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
		
	
}
