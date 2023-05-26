import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.lang.InterruptedException;

public class sRed extends Reducer<Text,IntWritable,Text,IntWritable>{
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
	{
		int sum=0;
		for(IntWritable val:values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}

}
