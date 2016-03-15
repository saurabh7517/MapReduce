package foo.org;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondaryReducer extends Reducer<CompositeKey,Text,Text,Text>{

	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values,
			Reducer<CompositeKey, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text value : values){
			context.write(new Text(key.toString()), new Text(value));
		}
	}
	
}
