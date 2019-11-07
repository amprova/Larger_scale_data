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

public class MoviePerRating {
	public static class MovieMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text MID = new Text();
		private final static LongWritable one = new LongWritable(1);


		public void map(Object key, Text val, Context context)
				throws IOException, InterruptedException
		{
			// input data format => movie_ID    (data is :: separated)
			String[] tokens = val.toString().split("::"); 
			String movie = tokens[1];
			String rating = tokens[2];
			//StringTokenizer itr = new StringTokenizer(movie);
			//itr.hasMoreTokens())
			if(rating.equals("1"))
			{
				MID.set(movie);
				context.write(MID, new Text(rating));
			}
		}
	}
	
	public static class MovieNameMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text MovieID = new Text();
		private final static Text MovieName = new Text();
		
		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			
			String rows = val.toString(); 
			String[] tokens = rows.split("::");
			String movieID = tokens[0];
			MovieID.set(movieID);
			String title = tokens[1];
			MovieName.set(title);
			context.write(MovieID, MovieName);

		}
	}

	public static class MovieReducer extends Reducer<Text, Text, Text, Text>
	{
		
		private Text result = new Text();
		private final static Text MovieName = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			 
			Iterator<Text> itr = values.iterator();
			//TreeMap<String,LongWritable> tmap = new TreeMap<String, LongWritable>();
			int sum = 0;
			while(itr.hasNext())
			{
				String val = itr.next().toString();
				if(val.equals("1"))
				{
					sum+=1;
				}
				
				else
				{
					MovieName.set(val);
				}
			}
			if(sum>0)
			{
				result.set(Integer.toString(sum));
				context.write(MovieName, result);
			}
		}
	}

	public static class MovieSort extends Mapper<LongWritable, Text, Text, Text> { 
		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException 
		{
				String[] tokens = value.toString().split("\t"); 
				String moviename = tokens[0];
				String rating = tokens[1];
				String result = moviename + "::" + rating;
				context.write(new Text("one"), new Text(result));
		}
			 	
	} 


	public static class SortReducer extends Reducer<Text, Text, Text, Text>
	{
		

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			Iterator<Text> itr = values.iterator();
			TreeMap<Integer,String> tmap = new TreeMap<Integer, String>(Collections.reverseOrder());
			while(itr.hasNext())
			{
				String result = itr.next().toString();
				String[] val_MID = result.split("::");
				String mname = val_MID[0];
				String rating = val_MID[1];
				int ratings = (Integer.parseInt(rating));
				tmap.put(ratings, mname);
			}
			for (Map.Entry<Integer, String> entry : tmap.entrySet())
	        { 	
				
	            String s = entry.getValue();
	            int key1 = entry.getKey();
	            String rate = Integer.toString(key1);
	            context.write(new Text(rate), new Text(s));
				
	        }

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
		
		//conf1.setInt("topn", Integer.parseInt(args[2]));
		Job job1 = new Job(conf1, "Popular but worst");
		job1.setJarByClass(MoviePerRating.class);
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
		//conf2.setInt("topn", Integer.parseInt(args[2]));
		Job job2 = new Job(conf2, "Popular but worst");
		job2.setJarByClass(MoviePerRating.class);
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


