import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.StringTokenizer;
import java.io.IOException;
import java.lang.InterruptedException;

public class mapper extends Mapper<Object,Text,Text,IntWritable>{
	public void map(Object offset,Text key,Context con) throws IOException,InterruptedException
	{
		StringTokenizer token = new StringTokenizer(key.toString()," - - ");
		//while(token.hasMoreElements())
		//{
			con.write(new Text(token.nextToken()),new IntWritable(1));
		//}
	}

}
