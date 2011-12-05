package pvuv;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author cuitao
 *计算每个新闻的pvuv
 */
public class PvuvReducer extends Reducer<Text,IntWritable,Text,Text>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {
		Set<Integer> userSet = new HashSet<Integer>();
		int pv = 0;
		for (IntWritable value : values) {
			pv++;
			userSet.add(value.get());
		}
		int uv = userSet.size();
		String value = pv + "," + uv;
		try {
			context.write(key, new Text(value));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
