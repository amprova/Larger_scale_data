import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.HashMap; 
import java.util.Map; 
import java.util.TreeMap;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

//import TopKMovieName.MovieMapper;
//import YearMovieName.MovieNameMapper;

import org.apache.hadoop.util.GenericOptionsParser;

public class ActiveAge {
	public static class MovieMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text UID = new Text();
		//private final static LongWritable one = new LongWritable(1);


		public void map(Object key, Text val, Context context)
				throws IOException, InterruptedException
		{
			// input data format => movie_ID    (data is :: separated)
			String[] tokens = val.toString().split("::"); 

			String user = tokens[0];
			StringTokenizer itr = new StringTokenizer(user);
			while (itr.hasMoreTokens()) {
				UID.set(itr.nextToken());
				context.write(UID, new Text("one"));
			}

		}
	}
	
	public static class MovieNameMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text UID = new Text();
		private final static Text Age = new Text();
		
		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			
			String rows = val.toString(); 
			//StringTokenizer itr = new StringTokenizer(rows);
			String[] tokens = rows.split("::");
			String userID = tokens[0];
			UID.set(userID);
			String age = tokens[2];
			
			Age.set(age);
			context.write(UID, Age);

		}
	}

	public static class MovieReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
		private final static Text Age = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			//String[] tokens = value.toString().split("\t"); 
			Iterator<Text> itr = values.iterator();
			int sum = 0;
			int one = 1;
			while(itr.hasNext())
			{
				String val = itr.next().toString();
				if(val.contentEquals("one"))
				{
					sum += one;
				}
				else
				{
					Age.set(val);
				}
				
			}
			result.set(Integer.toString(sum));
			context.write(Age, result);
		}
	}

	public static class MovieSort extends Mapper<LongWritable, Text, Text, Text> { 
		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException 
		{
				String[] tokens = value.toString().split("\t"); 
				String age = tokens[0];
				String count = tokens[1];
				context.write(new Text(age), new Text(count));

		}
			 	
	} 


	public static class SortReducer extends Reducer<Text, Text, Text, Text>
	{
		

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			Iterator<Text> itr = values.iterator();
			int activity = 0;
			while(itr.hasNext())
			{
				String count = itr.next().toString();
				int freq = Integer.parseInt(count);
				activity += freq;
			
			}
			context.write(key,new Text(Integer.toString(activity)));
			
		}
	}

	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException
	{
		Configuration conf1 = new Configuration();
		if (args.length < 3) {
			System.out.println("Usage: TopKmovies <input path> <input path> <output path>");
			System.exit(1);
		}
		
		Job job1 = new Job(conf1, "User activity by gender");
		job1.setJarByClass(ActiveAge.class);
	
		MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, MovieNameMapper.class);
		
		job1.setReducerClass(MovieReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		Path outputPath=new Path("FirstMapper");
		FileOutputFormat.setOutputPath(job1,outputPath);
		outputPath.getFileSystem(conf1).delete(outputPath);
		job1.waitForCompletion(true);
		

		Configuration conf2 = new Configuration();
		//conf2.set("topn", args[2]);
		Job job2 = new Job(conf2, "User activity by gender");
		job2.setJarByClass(ActiveAge.class);
		job2.setMapperClass(MovieSort.class);
		//job2.setNumReduceTasks(1);
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}

