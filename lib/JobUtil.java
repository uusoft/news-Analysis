package lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class JobUtil {

	public static Job prepareJob(String jobName,Configuration conf, Class<? extends Mapper> mapperClass,
			Class mapOutputKeyClass, Class mapOutputValueClass, Class<? extends Reducer> reducerClass, Class outputKeyClass,Class outputValueClass) throws IOException {
		Job job = new Job(conf,jobName);
		job.setMapperClass(mapperClass);
		job.setMapOutputKeyClass(mapOutputKeyClass);
		job.setMapOutputValueClass(mapOutputValueClass);
		job.setReducerClass(reducerClass);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);

		return job;
	}

}
