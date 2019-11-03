import java.io.IOException;

import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.HashMap; 
import java.util.Map; 
import java.util.TreeMap;

/**
 * Builds an Better inverted index: each word followed by files it was found in with frequency.
 * 
 * 
 * @author amifaraj
 */
public class year_popular
{

	public static class MovieMapper extends Mapper<Object, Text, Text, Text>


	{
		private final static Text Year = new Text();
		private final static Text MovieID = new Text();
		
		public void map(Object key, Text val, Context context) throws IOException, InterruptedException
		{
			
			String rows = val.toString(); 

			StringTokenizer itr = new StringTokenizer(rows);
			while (itr.hasMoreTokens()) {
				String[] tokens = itr.nextToken().split("::");
				int time = Integer.parseInt(tokens[3]);
				String movieID = tokens[1];
				MovieID.set(movieID);
				int year = (time/(365*24*60*60))+1970;
				String y = Integer.toString(year);
				Year.set(y);
				context.write(Year, MovieID);
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
			 
			int count = 0;
			int max = 10;
			boolean first = true;
			for(Map.Entry<Integer, List<String>> doc : Tree.entrySet()) {
				if (count > max) break;
			    int key1 = doc.getKey();
			    List<String> value = doc.getValue();
			    for (String s: value)
			    {
			    	if (!first)
				    	toReturn.append(", ");
				    first = false;
				    toReturn.append(s);   
				    count++;
			    }   
			    //List<String> top10 = new ArrayList<String>(value.subList(value.size() -10, value.size()));
			    //String documents = Arrays.toString(value.toArray()).replace("[", "").replace("]", "");
			    //count++;
			    //toReturn.append(value +" "+key1);
			}
			context.write(key, new Text(toReturn.toString()));
	        }
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		if (args.length < 2) {
			System.out
					.println("Usage: InvertedIndex <input path> <output path>");
			System.exit(1);
		}
		Job job = new Job(conf, "Top 10 movies by year");
		job.setJarByClass(year_popular.class);
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
