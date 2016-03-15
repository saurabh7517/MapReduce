package foo.org;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<CompositeKey,NullWritable> {

	@Override
	public int getPartition(CompositeKey ck, NullWritable value, int numOfPartitions) {
		// TODO Auto-generated method stub
		return ck.getNaturalKey().hashCode() & Integer.MAX_VALUE % numOfPartitions;
	}

}
