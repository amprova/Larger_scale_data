


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.util.*; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

public class SortedPopular {

	public static class MovieMapper extends Mapper<Object, Text, Text, LongWritable>


	{
		private final static Text MID = new Text();
		private final static LongWritable one = new LongWritable(1);

		//private final static int count = new Integer();


		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			// input data format => movie_ID    (data is :: separated)
			String[] tokens = val.toString().split("::"); 

			String movie = tokens[1];
			StringTokenizer itr = new StringTokenizer(movie);
			while (itr.hasMoreTokens()) {
				MID.set(itr.nextToken());
				context.write(MID, one);
			}
			//MID.set(movie);

		}
	}
	
	public static class MovieSort extends Mapper<Object, Text, String, String> 
	{ 
		
		//private List<String> MID_frq = new ArrayList<String>();
		
		public void map(Object key, Text value, Context context) throws IOException,  InterruptedException 
		{
			
			String input = value.toString();
			//String movie = input[0];
			//String count = input[1];
			StringTokenizer itr = new StringTokenizer(input);
			while (itr.hasMoreTokens()) {
				String[] id = itr.nextToken().split(" ");
				String movie = id[0];
				String count = id[1];
				//ID.set(itr.nextToken());
				context.write(count, movie);
			}
			//context.write(value, key);
			
			/*Iterator<LongWritable> itr = value.iterator();
			String val = itr.next().toString();
			String MID = key.toString();
			MID_frq.add(val+MID);
			context.write(null, MID_frq);*/
		} 
	}
	
	public static class MovieReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
		
	
	

	/*public static class SortReducer extends Reducer<LongWritable, List<String>,  List<String>, LongWritable>
	{
		

		public void reduce(LongWritable key, List<String> values, Context context)
				throws IOException, InterruptedException
		{
			List<String> value = values;
		    Collections.sort(value);
		    context.write(value, key); 
			
			//TreeMap<String,LongWritable> tmap = new TreeMap<String, LongWritable>(Collections.reverseOrder());
			
			//for(String s: values)
			//{
				//tmap.put(s, key);
				
			}
			for (Map.Entry<String, Long> entry : tmap.entrySet())  
	        { 
	  
	            String s = entry.getKey(); 
	            Text name = entry.getValue(); 
	            context.write(new String(s), new Text(name)); 
	        } 
		}*/

	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException
	{
		Configuration conf1 = new Configuration();
		if (args.length < 2) {
			System.out
			.println("Usage: TopKmovies <input path> <output path>");
			System.exit(1);
		}
		Job job1 = new Job(conf1, "Top k Movies");
		job1.setJarByClass(SortedPopular.class);
		job1.setMapperClass(MovieMapper.class);
		//job1.setCombinerClass(MovieReducer.class);
	    job1.setCombinerClass(MovieReducer.class);
		job1.setReducerClass(MovieReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outputPath=new Path("firstMap4");
		FileOutputFormat.setOutputPath(job1,outputPath);
		//outputPath.getFileSystem(conf1).delete(outputPath);
		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Sorted Top k Movies");
		job2.setJarByClass(SortedPopular.class);
		job2.setMapperClass(MovieSort.class);
		//job2.setReducerClass(SortReducer.class);
		job2.setNumReduceTasks(1);
		//job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job2.setOutputKeyClass(String.class);
		job2.setOutputValueClass(String.class);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}

