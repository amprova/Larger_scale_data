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

public class PopularGenre {
	public static class MovieMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text MID = new Text();
		//private final static LongWritable one = new LongWritable(1);


		public void map(Object key, Text val, Context context)
				throws IOException, InterruptedException
		{
			// input data format => movie_ID    (data is :: separated)
			String[] tokens = val.toString().split("::"); 

			String movie = tokens[1];
			StringTokenizer itr = new StringTokenizer(movie);
			while (itr.hasMoreTokens()) {
				MID.set(itr.nextToken());
				context.write(MID, new Text("one"));
			}

		}
	}
	
	public static class MovieNameMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text MovieID = new Text();
		private final static Text Genre = new Text();
		
		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			
			String rows = val.toString(); 
		
			String[] tokens = rows.split("::");
			String movie = tokens[0];
			MovieID.set(movie);
			String genre = tokens[2];
			Genre.set(genre);
			context.write(MovieID, Genre);

		}
	}

	public static class MovieReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
		private final static Text Genre = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			
			Iterator<Text> itr = values.iterator();
			int sum = 0;
			int one = 1;
			String[] genrelist = null;
			while(itr.hasNext())
			{
				String val = itr.next().toString();
				if(val.contentEquals("one"))
				{
					sum += one;
				}
				else
				{
					//Genre.set(val);
					genrelist = val.split("\\|");
				}

			}
			result.set(Integer.toString(sum));
			for(String g: genrelist)
			{
				context.write(new Text(g), result);
			}

			//context.write(Genre, result);
		}
	}

	public static class MovieSort extends Mapper<LongWritable, Text, LongWritable, Text> { 
		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException 
		{
				LongWritable one = new LongWritable(1);
				context.write(one, value);
		}
			 	
	} 

	public static class SortReducer extends Reducer<LongWritable, Text, Text, Text>
	{
		

		public void reduce(LongWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			Iterator<Text> itr = values.iterator();
			
			HashMap<String, Integer> frequency = new HashMap<>();
			
			while(itr.hasNext())
			{
				String[] tokens = itr.next().toString().split("\t"); 
				String genre = tokens[0];
				String count = tokens[1];
				
				if(frequency.containsKey(genre))
				{
					int prev = frequency.get(genre);
					int freq = Integer.parseInt(count);
					int total = prev+freq;
					frequency.put(genre, total);
				}
				else
				{
					int freq = Integer.parseInt(count);
					frequency.put(genre, freq);
				}
			}
			
			StringBuilder toReturn = new StringBuilder();
			
			Map<Integer, String> Tree = new TreeMap<>(Collections.reverseOrder());
			
			for (Map.Entry<String, Integer> treeMap : frequency.entrySet()) { 
				int val = treeMap.getValue();
				String s = treeMap.getKey();
				Tree.put(val, s);
			}
			for (Map.Entry<Integer, String> entry : Tree.entrySet())
	        { 	
	            String s = entry.getValue();
	            Integer freq = entry.getKey(); 
	            context.write(new Text(Integer.toString(freq)),new Text(s));
				
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
		//conf1.set("topn", args[2]);
		Job job1 = new Job(conf1, "Top k Movies");
		job1.setJarByClass(PopularGenre.class);
	
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
		Job job2 = new Job(conf2, "Sorted Top k Movies");
		job2.setJarByClass(PopularGenre.class);
		job2.setMapperClass(MovieSort.class);
		//job2.setNumReduceTasks(1);
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}


