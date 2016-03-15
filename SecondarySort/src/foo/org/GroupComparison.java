package foo.org;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparison extends WritableComparator {
	protected GroupComparison(){
		super(CompositeKey.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		CompositeKey c1 = (CompositeKey) a;
		CompositeKey c2 = (CompositeKey) b;
		return c1.getNaturalKey().compareTo(c2.getNaturalKey());
	}
	
}
