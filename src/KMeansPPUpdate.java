import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

public class KMeansPPUpdate
{
	public enum Counter
	{
		CONVERGED
	}
	
	public static Job SetupJob(String input, String oldOutput, String output)
			throws Exception
	{		
		Configuration conf = new Configuration();
		conf.set("output", oldOutput);
		Job job = Job.getInstance(conf, "KMeansPPUpdate");
		job.setJarByClass(KMeansPPUpdate.class);
		
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		Path inputPath = new Path(input);
		Path outputPath = new Path(output);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job;
	}
	
	public static class CustomMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		private IntWritable zero = new IntWritable(0);
		private Text data = new Text();
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
			for(Map.Entry<Integer, Point> entry : centroids.entrySet())
			{
				Double currDist = point.calcDistance(entry.getValue());
				if(currDist < minDist)
				{
					minDist = currDist;
				}
			}
			data.set(point.toString() + ":" + minDist.toString());
			ctx.write(zero, data);
		}
		
	}
	
	public static class CustomReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		private Text newCenter = new Text();
		private IntWritable clusterID = new IntWritable();
		private HashMap<Integer, Point> centroids = new HashMap<Integer, Point>();
		private Random rand = new Random();
		
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
		
		public void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
		{
			ArrayList<String> points = new ArrayList<String>();
			ArrayList<Double> distances = new ArrayList<Double>();
			Double sum = 0.0;
			
			for(Text value: values)
			{
				String[] line = value.toString().split(":");
				points.add(line[0]);
				Double dist = Double.parseDouble(line[1]);
				distances.add(dist);
				sum += dist;
			}
			
			Double randDist = rand.nextDouble() * sum; // [0,1] sum(dist)
			Integer j = -1;
			
			while(randDist >= 0) 
			{
				j++;
				randDist -= distances.get(j);		
			}
			
			for(Map.Entry<Integer, Point> entry : centroids.entrySet()) // emit old centroids
			{
				clusterID.set(entry.getKey());
				newCenter.set(entry.getValue().toString());
				ctx.write(clusterID, newCenter);
			}
			
			clusterID.set(centroids.size());
			newCenter.set(points.get(j));
			ctx.write(clusterID, newCenter);
		}
	}
}
