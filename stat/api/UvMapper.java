package stat.api;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UvMapper extends Mapper<Text,Text,Text,Text>{

	@Override
	public void map(Text apiName, Text value, Context context) {
		if (apiName.toString().contains(".go")) {
			try {
				String[] tmp = value.toString().split("\t");
				if (tmp.length <=0)
					return;
				String uid = tmp[0];
				context.write(new Text("apistat"), new Text(uid));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main (String[] args) {
		String line = "/api/welcome.do	9999973		2011-12-13	11:24:13";
		String[] tmp = line.toString().split("\t");
		
		if (line.contains(".go"))
			System.out.println(line);
	}
}
