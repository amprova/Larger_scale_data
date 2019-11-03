import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;



 /* 
 * 
 * 
 * 
 */

public class YearMovieName
{

	public static class MovieMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text Year = new Text();
		private final static Text Movie = new Text();
		
		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			
			String rows = val.toString(); 

			StringTokenizer itr = new StringTokenizer(rows);
			while (itr.hasMoreTokens()) {
				String[] tokens = itr.nextToken().split("::");
				int time = Integer.parseInt(tokens[3]);
				String movie = tokens[1];
				Movie.set(movie);
				int year = (time/(365*24*60*60))+1970;
				String y = Integer.toString(year);
				Year.set(y);
				context.write(Year, Movie);
			}
			//MID.set(movie);

		}
	}
	

	public static class MovieReducer extends Reducer<Text, Text, Text, Text>
	{

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			Iterator<Text> itr = values.iterator();

			/**Better Inverted Index using hashmap
			counting the term frequency per document
			***/
			
			HashMap<String, Integer> frequency = new HashMap<>();
			//calculating number of appearance of a document. <doc, count> HashMap
			while(itr.hasNext())
			{
				String document = itr.next().toString(); 
				if(frequency.containsKey(document))
				{
					int count = frequency.get(document);
					frequency.put(document, count+1);
				}
				else
				{
					frequency.put(document, 1);
				}
			}
			
			boolean first = true;
			StringBuilder toReturn = new StringBuilder();
			
			// TreeMap stores sorted by key 
			Map<Integer, List<String>> Tree = new TreeMap<>(Collections.reverseOrder());
			
			for (Map.Entry<String, Integer> treeMap : frequency.entrySet()) { 
				int val = treeMap.getValue();
				if(Tree.containsKey(val))
				{
					List<String> s = Tree.get(val);
					String docToadd = treeMap.getKey().toString();
					s.add(val+" "+docToadd);
					Tree.put(val, s);
				}
				else
				{
					List<String> s = new ArrayList<String>();
					String docToadd = treeMap.getKey().toString();
					s.add(val+" "+docToadd);
					Tree.put(val, s);
				}
				 
	        }
			
			
			for(Map.Entry<Integer, List<String>> doc : Tree.entrySet()) {
			    int key1 = doc.getKey();
			    List<String> value = doc.getValue();
			    Collections.sort(value);
			    String documents = Arrays.toString(value.toArray()).replace("[", "").replace("]", "");
			    
			    if (!first)
			    	toReturn.append(", ");
			    first = false;
			    //toReturn.append(value +" "+key1);
			    toReturn.append(documents);
			}
			
			context.write(key, new Text(toReturn.toString()));
	        }
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		if (args.length < 3) {
			System.out
					.println("Usage: Topk <input path> <input path> <output path>");
			System.exit(1);
		}
		//Job job = new Job(conf, "YearName");
		Job job = Job.getInstance(conf);
		job.setJarByClass(YearMovieName.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, MovieMapper.class);
		//job.setJarByClass(Multiplication.class);
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

	    //MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, JoinMapper.class);

		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


public class PopularByName extends Configured implements Tool
{

    public static class MergeListenersMapper extends Mapper<IntWritable, IntWritable, IntWritable, TrackStats>
    {

	public void map(IntWritable trackId, IntWritable uniqueListenerCount, Context context)
	        throws IOException, InterruptedException {
	    TrackStats trackStats = new TrackStats();
	    trackStats.setListeners(uniqueListenerCount.get());
	    context.write(trackId, trackStats);
	}
    }


    public int run(String[] args) throws Exception {

	Configuration conf = new Configuration();

	Path sumInputDir = new Path(args[1] + Path.SEPARATOR_CHAR + "sumInputDir");
	Path listenersInputDir = new Path(args[1] + Path.SEPARATOR_CHAR + "listenersInputDir");
	Path output = new Path(args[1] + Path.SEPARATOR_CHAR + "finalSumOutput");

	@SuppressWarnings("deprecation")
	Job job = new Job(conf, "merge-results");
	job.setJarByClass(UniqueListeners.class);

	MultipleInputs.addInputPath(job, sumInputDir, SequenceFileInputFormat.class, Mapper.class);
	MultipleInputs.addInputPath(job, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);

	job.setReducerClass(SumTrackStats.SumTrackStatsReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(TrackStats.class);

	FileOutputFormat.setOutputPath(job, output);
	if (job.waitForCompletion(true))
	    return 0;
	else
	    return 1;
    }
}
