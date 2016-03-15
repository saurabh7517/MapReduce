package com.foo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistMapper extends Mapper<LongWritable,Text,IntWritable, Text>{
	
	private static final IntWritable customKey = new IntWritable();
	private static final Text customValue = new Text();
	private HashMap<String, String> join ;
	private BufferedReader brReader;


	@Override
	protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String tempKey;
		String line = value.toString();
		String stringArray[] = line.split(",");
		tempKey = stringArray[0];

		if(join.containsKey(tempKey)){
			customKey.set(Integer.parseInt(tempKey));
			customValue.set(line + join.get(tempKey));

			
		}
		context.write(customKey, customValue);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		join = new HashMap<String, String>();
		String lineRead = "";
		URI[] cacheFile = DistributedCache.getCacheFiles(context.getConfiguration());
		File lookupFile = new File(cacheFile[0]);
		try {
			brReader = new BufferedReader(new FileReader(lookupFile));
			System.out.println(lookupFile.toString());
			// Read each line, split and load to HashMap
			while ((lineRead = brReader.readLine()) != null) {
				System.out.println(lineRead);
				String projectFieldArray[] = lineRead.split(",");
				System.out.println(projectFieldArray[0]);
				join.put(projectFieldArray[0].trim(),
						lineRead);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		System.out.println(join.get("1234"));
	}
	
}
