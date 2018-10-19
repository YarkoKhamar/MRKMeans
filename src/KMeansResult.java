import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansResult
{
	public static Job SetupJob(String input, String oldOutput, String output)
			throws Exception
	{		
		Configuration conf = new Configuration();
		conf.set("output", oldOutput);
		Job job = Job.getInstance(conf, "CentroidUpdate");
		job.setJarByClass(KMeansUpdate.class);
		
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job;
	}
	
	public static class CustomMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		private IntWritable cluster = new IntWritable();
 		private HashMap<Integer, Point> centroids = new HashMap<Integer, Point>();
		
		public void setup(Context ctx) throws IOException
		{
			Configuration conf = ctx.getConfiguration();
			String path = conf.get("output");
			String file = path + "/part-r-00000";
			
			Path filePath = new Path(file);
			FileSystem fs = filePath.getFileSystem(conf);
			FSDataInputStream stream = fs.open(filePath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
			try
			{
				String line = null;
				while((line = reader.readLine()) != null)
				{
					String[] cluster = line.split("\t");
					Point center = new Point(cluster[1]);
					centroids.put(Integer.parseInt(cluster[0]), center);
				}
			}
			finally
			{
				reader.close();
			}
		}
		
		public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException
		{
			Point point = new Point(value.toString());
			Double minDist = Double.POSITIVE_INFINITY;
			Integer minCluster = -1;
			for(Map.Entry<Integer, Point> entry : centroids.entrySet())
			{
				Double currDist = point.calcDistance(entry.getValue());
				if(currDist < minDist)
				{
					minDist = currDist;
					minCluster = entry.getKey();
				}
			}
			
			cluster.set(minCluster);
			ctx.write(cluster, value);
		}
		
	}
	
	public static class CustomReducer extends Reducer<IntWritable, Text, Text, IntWritable>
	{
		public void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
		{
			for(Text value: values)
			{
				ctx.write(value, key);
			}
		}
	}
}
