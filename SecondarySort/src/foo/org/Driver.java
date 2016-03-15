package foo.org;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Job job = new Job();
		
		job.setJarByClass(Driver.class);
		
		job.setMapperClass(SecondaryMapper.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(SecondaryReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparison.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
	}

}
