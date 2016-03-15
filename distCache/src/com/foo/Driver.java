package com.foo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Driver.class);
		job.setMapperClass(DistMapper.class);
		job.setReducerClass(DistReducer.class);
		
//		job.setInputFormatClass(TextInputFormat.class)		;
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		URI uri = new URI(args[2]);
		
		DistributedCache.addCacheFile(uri, conf);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
