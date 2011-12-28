package pref;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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
