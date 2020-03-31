package org.queens;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Analysis extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Analysis(), args);
	    System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "analysis");
		job.addCacheFile(new Path(args[0]).toUri());
		job.addCacheFile(new Path(args[1]).toUri());
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
	    int result = job.waitForCompletion(true) ? 0 : 1;
	    return result;
	}
}
