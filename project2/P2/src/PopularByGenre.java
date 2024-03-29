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

public class PopularByGenre {
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
	
	public static class MovieNameAndGenreMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text MovieID = new Text();
		private final static Text MovieInfo = new Text();
		
		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			
			String rows = val.toString(); 
			//StringTokenizer itr = new StringTokenizer(rows);
			String[] tokens = rows.split("::");
			String movie = tokens[0];
			MovieID.set(movie);
			String title = tokens[1];
			String genre = tokens[2];
			String movieinfo = title + "::" +genre;
			
			MovieInfo.set(movieinfo);
			context.write(MovieID, MovieInfo);

		}
	}

	public static class MovieReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
		private final static Text MovieInfo = new Text();

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
					MovieInfo.set(val);
				}
				
			}
			result.set(Integer.toString(sum));
			context.write(MovieInfo, result);
		}
	}

	public static class MovieSort extends Mapper<LongWritable, Text, Text, Text> { 
		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException 
		{
				String[] tokens = value.toString().split("\t"); 
				String moviename = tokens[0];
				String count = tokens[1];
				String[] movieinfo = moviename.split("::");
				String title = movieinfo[0];
				String genre = movieinfo[1];
				String[] genrelist = genre.split("\\|");
				for(String g: genrelist)
				{
					String s = count+"::"+title;
					context.write(new Text(g), new Text(s));
				}
		}
			 	
	} 


	public static class SortReducer extends Reducer<Text, Text, Text, Text>
	{
		

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			Iterator<Text> itr = values.iterator();
			TreeMap<LongWritable,String> tmap = new TreeMap< LongWritable, String>(Collections.reverseOrder());
			
			while(itr.hasNext())
			{
				String document = itr.next().toString();
				String[] val_MID = document.split("::");
				tmap.put(new LongWritable(Integer.parseInt(val_MID[0])), val_MID[1]);
				
				
			}
			Map.Entry<LongWritable, String> popular =  tmap.firstEntry();
			String popFreq = "Frequency: "+popular.getKey().toString();
			String popName = "Title: "+popular.getValue();
			context.write(key,new Text("\n"+"Most Popular Movie: "+"\t"+popFreq+"; "+popName));
			
			Map.Entry<LongWritable, String> worst =  tmap.lastEntry();
			String leastFreq = "Frequency: "+worst.getKey().toString();
			String leastName = "Title: "+worst.getValue();
			context.write(key,new Text("\n"+"Least Popular Movie: "+"\t"+leastFreq+"; "+leastName));
		}
	}

	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException
	{
		Configuration conf1 = new Configuration();
		if (args.length < 3) {
			System.out.println("Usage: PopularByGenre <input path> <input path><output path>");
			System.exit(1);
		}
		
		//conf1.setInt("topn", Integer.parseInt(args[2]));
		Job job1 = new Job(conf1, "Popular By Genre");
		job1.setJarByClass(PopularByGenre.class);
		MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, MovieNameAndGenreMapper.class);
	
		job1.setReducerClass(MovieReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputPath=new Path("FirstMapper");
		FileOutputFormat.setOutputPath(job1,outputPath);
		outputPath.getFileSystem(conf1).delete(outputPath);
		job1.waitForCompletion(true);
		

		Configuration conf2 = new Configuration();
		//conf2.setInt("topn", Integer.parseInt(args[2]));
		Job job2 = new Job(conf2, "Popular By Genre");
		job2.setJarByClass(PopularByGenre.class);
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


