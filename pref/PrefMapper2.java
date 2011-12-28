package pref;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrefMapper2 extends Mapper<Text,Text,Text,Text>{

	@Override
	public void map (Text uid1, Text uid2, Context context) {
		try {
			context.write(uid1, uid2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
