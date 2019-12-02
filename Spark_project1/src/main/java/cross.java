
/**
 *  
 *  Calculate cross correlation of a list of items.
 *  Given a set of 2-tuples of items. For each possible pair of items 
 *  calculate the number of tuples where these items co-occur. 
 *  If the total number of items is n, then n^2 = n × n values are reported. 
 *  This is the started example.
 *
 *  @author amit 
 *  
 */

import scala.Tuple2;
import java.util.*; 

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class cross
{
    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void main(String[] args) throws Exception {

	SparkConf conf = new SparkConf().setAppName("CrossCorrelation");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(args[0]);
	
	/** change to make */
	JavaPairRDD<String, Long> pairs = lines.zipWithUniqueId();
	JavaRDD<String> words = pairs.flatMap(f -> 
	{
		ArrayList<String> list = new ArrayList<String>();
		String[] word = SPACE.split(f._1);  //get the words in the file
		
		for(int i = 0; i<word.length;i++)
		{
			list.add(f._2.toString()+"::"+word[i]);
		}
		return list.iterator();
	});
	JavaPairRDD<String, String> cart = words.cartesian(words);
	JavaPairRDD<String, String> filtered = cart.filter(new Function <Tuple2<String,String>,Boolean>()
	{
		public Boolean call(
		Tuple2<String, String>pairs)
		{
			String a = pairs._1;
			String b = pairs._2;
			String unique1 = a.split("::")[0];
			String unique2 = b.split("::")[0];
			String content1 = a.split("::")[1];
			String content2 = b.split("::")[1];
			int comp = content1.compareTo(content2);
			if(content1.equals(content2) || (!unique1.equals(unique2) || comp>1))
			{
					return false;
			}
			else
			{
				return true;
			}
		}
	});
	JavaPairRDD<String, String> newpair = filtered.mapToPair(s -> 
	{
		String pair1 = s._1.toString().split("::")[1];
		
		String pair2 = s._2.toString().split("::")[1];
	
		return new Tuple2<String, String>(pair1,pair2);
	});
	JavaPairRDD<Tuple2<String, String>, Integer> correlation = newpair.mapToPair(s -> new Tuple2(s, 1));
	correlation = correlation.reduceByKey((x, y) -> x + y);
	JavaPairRDD<Integer, Tuple2<String, String>> swapped = correlation.mapToPair(s -> s.swap());
	swapped = swapped.sortByKey(false);
	
	List<Tuple2<Integer, Tuple2<String, String>>> output =swapped.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
		System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
	
	sc.stop();
	sc.close();
    }
}
