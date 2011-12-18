package itemBaseRecommendation2;

import lib.ItemCooccurrence;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WrapCoJob {

	public static class WrapCoMapper extends Mapper<Text,ItemCooccurrence,Text,CoAndPref> {
		@Override
		public void map (Text nid, ItemCooccurrence ic, Context context) {
			CoAndPref cap = new CoAndPref();
		}
	}
}
