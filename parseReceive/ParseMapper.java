package parseReceive;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ParseMapper extends Mapper<Object, Text, Text, Text>{
	
	/* (non-Javadoc)
	 * 输入key	无
	 * 输入value	一行nignxlog
	 * 输出key-value格式: termId+","+platform -> userId
	 */
	@Override
	public void map(Object key, Text value, Context context) {
		
		String line = value.toString();
		List<String> info = Utils.parseUrl(line);
		if (info == null || info.size() < 3)
			return;
		
		String userId = info.get(0);
		for (int i=2; i<info.size(); i++) {
			try {
				String termId = info.get(i);
				context.write(new Text(userId), new Text(termId));
			} catch (Exception e) {

				e.printStackTrace();
			} 
		}
		
		
	}

}
