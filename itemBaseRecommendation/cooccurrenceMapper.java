package itemBaseRecommendation;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class cooccurrenceMapper extends Mapper<Text,ArrayWritable,Text,Text>{
	
	@Override
	public void map(Text uid,ArrayWritable nidList, Context context) {
		
		for (Text t : (Text[])nidList.toArray()) {
			System.out.println(uid+":"+t);
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
