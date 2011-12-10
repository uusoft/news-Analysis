package itemBaseRecommendation;

import java.io.IOException;
import java.util.List;

import lib.ListWritable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class CooccurrenceMapper extends Mapper<Text,ListWritable,Text,Text>{
	
	@Override
	public void map(Text uid,ListWritable nidList, Context context) {
		
		List<Writable> lw = nidList.get();
		for (Writable w : lw) {
			Text nid = (Text) w;
			
		}
		
		for (int i=0; i<lw.size(); i++) {
			for (int j=i+1; j<lw.size(); j++) {
				try {
					context.write((Text) lw.get(i), (Text) lw.get(j));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
//		for (Text nid: nidList) {
//			Iterator<Text> i = nidList.iterator();
//			while(i.hasNext()) {
//				Text nid2 = i.next();
//				try {
//					context.write(nid, nid2);
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
// 		}
	}

}
