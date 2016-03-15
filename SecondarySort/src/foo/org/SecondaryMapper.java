package foo.org;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondaryMapper extends Mapper<LongWritable,Text,CompositeKey,Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CompositeKey, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		String[] tokens = line.split(",");

		
		context.write(new CompositeKey(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[3])), new Text(line));
	}
	
}
