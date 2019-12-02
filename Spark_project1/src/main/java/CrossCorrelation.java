
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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class CrossCorrelation
{
    private static final Pattern SPACE = Pattern.compile("\\s+");


    public static void printPairRDD(JavaPairRDD<String, String> rdd) {
	List<Tuple2<String, String>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
    }


    public static void printTripletRDD(JavaPairRDD<Tuple2<String, String>, Integer> rdd) {
	List<Tuple2<Tuple2<String, String>, Integer>> output = rdd.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
    }


    public static void main(String[] args) throws Exception {

	SparkConf conf = new SparkConf().setAppName("CrossCorrelation");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(args[0]);
	
	/** change to make */
	//JavaPairRDD<String, String> pairs = lines.mapToPair(s -> new Tuple2<>(SPACE.split(s)[0], SPACE.split(s)[1]));
	JavaRDD<Tuple2<String, String>> pairslist = lines.flatMap(s -> 
	{
		int i;
		int j;
		ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
		String[] words = SPACE.split(s);
		for(i=0; i<words.length-1;i++)
		{
			for(j=i+1;j<words.length;j++)
			{
				list.add(new Tuple2<String, String>(words[i], words[j])); 
			}
		}		
		return list.iterator();		
	});
	//JavaPairRDD<String,Long> pairs = lines.zipWithIndex(s -> new Tuple2<>(SPACE.split(s)));
	/*JavaPairRDD<String[],String> pairs = lines.mapToPair(s -> 
	{
		return new Tuple2<String[], String>(s.split(" "), "line");
	});*/
	//JavaRDD<String> list = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
	//JavaRDD<Object> words = lines.map(s -> SPACE.split(s).length);//((a,b) => if(a>b) a else b);
	//JavaRDD<String> words = lines.flatMap(s -> SPACE.split(s));
	//System.out.println(list.toString());
	//JavaPairRDD<String, String> cart = list.cartesian(list);
	//JavaPairRDD<String, String> cart = lines.cartesian(lines);
	//printPairRDD(pairs);
	/*JavaPairRDD<String, String> filtered = cart.filter(new Function <Tuple2<String,String>,Boolean>()
			{
		public Boolean call(
				Tuple2<String, String>pairs)
		{
			String a = pairs._1;
			String b = pairs._2;
			int comp = a.compareTo(b);
			if(comp < 0)
			{
				return true;
			}
			else
			{
				return false;
			}
		}
			});
	//printPairRDD(filtered);
	//printPairRDD(pairs);
	JavaPairRDD<Tuple2<String, String>, Integer> correlation = filtered.mapToPair(s -> new Tuple2(s, 1));*/
	JavaPairRDD<Tuple2<String, String>, Integer> correlation = pairslist.mapToPair(s -> new Tuple2(s, 1));
	printTripletRDD(correlation);

	correlation = correlation.reduceByKey((x, y) -> x + y);
	printTripletRDD(correlation);
	
	JavaPairRDD<Integer, Tuple2<String, String>> swapped = correlation.mapToPair(s -> s.swap());
	swapped = swapped.sortByKey(false);
	
	List<Tuple2<Integer, Tuple2<String, String>>> output = swapped.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
		System.out.println("(" + tuple._2() + "," + tuple._1() + ")");
	}
	System.out.println();
	
	sc.stop();
	sc.close();
    }
}
