package com.foo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistReducer extends Reducer<IntWritable,Text,IntWritable,Text>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text value : values){
			context.write(key,new Text(value));
//			System.out.println(key.toString() + value.toString());
		}
	}

}
