import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClusteringInitializer 
{
	public static Job SetupJob(String input, String output, Integer k) // Setup Hadoop Job
			throws Exception
	{		
		Configuration conf = new Configuration(); // ~= dictionary
		conf.setInt("k", k);
		Job job = Job.getInstance(conf, "ClusterInitialize"); // ~= Job constructor
		job.setJarByClass(ClusteringInitializer.class);
		
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(IntWritable.class); // output type mapper == reducer <int, string>
	 	job.setOutputValueClass(Text.class);
		
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job;
	}
	
	public static class CustomMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		private IntWritable cluster = new IntWritable(); // clusterID
		private Random rand = new Random();
		
		public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException
		{
			int k = ctx.getConfiguration().getInt("k", 0);	
			cluster.set(rand.nextInt(k));
			ctx.write(cluster, value);
		}		
	}
	
	public static class CustomReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		private Text newCenter = new Text();
		private Random rand = new Random();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
		{
			ArrayList<String> points = new ArrayList<String>();

			for(Text value: values)
			{
				points.add(value.toString());
			}

			newCenter.set(points.get(rand.nextInt(points.size())));
			ctx.write(key, newCenter);
		}
	}
}
