import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class count {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Wordcount");
		job.setJarByClass(count.class);
		job.setMapperClass(sMap.class);
		job.setReducerClass(sRed.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000" + args[1]),conf);

		Path study = fs.listStatus(new Path(args[1]))[1].getPath();

		FSDataInputStream in = null;

		in = fs.open(study);

		int max = 0;

	
		String obj;

		Scanner sc = new Scanner(in);

		String result = null;
		while(sc.hasNext()) {
			obj = sc.nextLine();
			String[] arrobj = obj.trim().split("\t+");
			int n = Integer.parseInt(arrobj[1]);
			if(n>max) {
				max = n;
				result  = obj;
			}
		}
		System.out.println(result);

	}
}
