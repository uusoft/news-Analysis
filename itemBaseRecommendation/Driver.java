package itemBaseRecommendation;

import org.apache.hadoop.util.ToolRunner;

public class Driver {

	public static void main (String[] args) {
		String step = args[0];
		String[] newArgs = new String[args.length-1];
		for (int i=1; i<args.length; i++) {
			newArgs[i-1] = args[i];
		}
		try {
			if ("1".equals(step)) {
				ToolRunner.run(new UserVectorJob(), newArgs);
			}
			else if ("2".equals(step)) {
				ToolRunner.run(new CooccurrenceJob(), newArgs);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
}
