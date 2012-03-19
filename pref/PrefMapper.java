package pref;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author cuitao
 * input:
 *1054632	,2011-12-11:09:37:11
1061085	9728479,2011-12-11:11:16:11
1070724	4717579,2011-12-11:10:42:11
1070724	4717579,2011-12-11:10:43:11
 */
public class PrefMapper extends Mapper<Text,Text,Text,Text>{
	@Override
	public void map (Text nid, Text line, Context context) {
		String[] tmp = line.toString().split(",");
		if (tmp.length != 2) {
			return;
		}
		String uid = tmp[0];
		
		try {
			context.write(nid, new Text(uid));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
