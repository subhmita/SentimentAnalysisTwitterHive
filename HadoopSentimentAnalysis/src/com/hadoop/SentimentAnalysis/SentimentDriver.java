package com.hadoop.SentimentAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SentimentDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SentimentAnalysis Preprocessing");
		job.setJarByClass(SentimentDriver.class);
		job.setMapperClass(SentimentAnalysisPreprocessingMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		org.apache.hadoop.filecache.DistributedCache.addFileToClassPath(new Path("/data001/json-simple-1.1.jar"), job.getConfiguration(),FileSystem.get(conf));
		
		if (dfs.exists(outPath))
		{
			dfs.delete(outPath, true);
		}
		job.waitForCompletion(true);
	}
	
}
