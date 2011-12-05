package tempAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	@Override
	public void map(LongWritable pos, Text line, Context context) {
		String[] tmp1 = line.toString().split("\t");
		if (tmp1.length != 2) 
			return;
		String[] tmp2 = tmp1[1].split(",");
		if (tmp2.length != 2)
			return;
		String uid = tmp2[0];
		String time = tmp2[1];
		if (time.equals(""))
			return;
		String[] tmp3 = time.split("/");
		if (tmp3.length<1)
			return;
		String day = tmp3[0];
		
		try {
			context.write(new Text(uid+"\t"+day), new IntWritable(1));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
