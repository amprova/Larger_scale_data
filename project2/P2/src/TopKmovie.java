
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
import static java.util.stream.Collectors.*;


import java.util.*; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

public class TopKmovie {
	public static class MovieMapper extends Mapper<Object, Text, Text, LongWritable>


	{
		private final static Text MID = new Text();
		private final static LongWritable one = new LongWritable(1);


		public void map(Object key, Text val, Context context)
				throws IOException, InterruptedException
		{
			// input data format => movie_ID    (data is :: separated)
			String[] tokens = val.toString().split("::"); 

			String movie = tokens[1];
			StringTokenizer itr = new StringTokenizer(movie);
			while (itr.hasMoreTokens()) {
				MID.set(itr.nextToken());
				context.write(MID, one);
			}

		}
	}

	public static class MovieReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class MovieSort extends Mapper<LongWritable, Text, LongWritable, Text> { 
		
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException 
		{
				LongWritable one = new LongWritable(1);
				String[] tokens = value.toString().split("\t"); 

				String movie = tokens[0];
				String count = tokens[1];
				String s = count+","+movie;
				context.write(one, new Text(s));
		}
			 	
	} 


	public static class SortReducer extends Reducer<LongWritable, Text, LongWritable, Text>
	{
		

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			Iterator<Text> itr = values.iterator();
			TreeMap<LongWritable,String> tmap = new TreeMap< LongWritable, String>(Collections.reverseOrder());
			
			while(itr.hasNext())
			{
				String document = itr.next().toString();
				StringTokenizer itr1 = new StringTokenizer(document);
				while(itr1.hasMoreTokens())
				{
					String[] val_MID = itr1.nextToken().toString().split(",");
					tmap.put(new LongWritable(Integer.parseInt(val_MID[0])), val_MID[1]);
					
				}
				
			}
			String topn = context.getConfiguration().get("topn");
			int max = Integer.parseInt(topn, 10);
			int count = 0;
			for (Map.Entry<LongWritable, String> entry : tmap.entrySet())
	        { 	
				if(count<max)
				{
	            String s = entry.getValue();
	            LongWritable freq = entry.getKey(); 
	            context.write(freq,new Text(s));
				}
				count++;
	        } 
		}
	}

	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException
	{
		Configuration conf1 = new Configuration();
		if (args.length < 3) {
			System.out.println("Usage: TopKmovies <input path> <topn> <output path>");
			System.exit(1);
		}
		conf1.set("topn", args[1]);
		Job job1 = new Job(conf1, "Top k Movies");
		job1.setJarByClass(TopKmovie.class);
		job1.setMapperClass(MovieMapper.class);
		job1.setReducerClass(MovieReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outputPath=new Path("FirstMapper");
		FileOutputFormat.setOutputPath(job1,outputPath);
		outputPath.getFileSystem(conf1).delete(outputPath);
		job1.waitForCompletion(true);
		

		Configuration conf2 = new Configuration();
		conf2.set("topn", args[1]);
		Job job2 = new Job(conf2, "Sorted Top k Movies");
		job2.setJarByClass(TopKmovie.class);
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

