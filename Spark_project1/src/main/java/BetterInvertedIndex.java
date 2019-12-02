/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* modified by amit */

import scala.Tuple2;

import org.apache.commons.collections.list.TreeList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Lists;

import java.util.regex.Pattern;
import java.util.*;
import java.util.Map.Entry; 

public class BetterInvertedIndex
{
    private static final Pattern SPACE = Pattern.compile(" ");
    
    public static void printPairRDD(JavaPairRDD<String, String> rdd) {
    	List<Tuple2<String, String>> output = rdd.collect();
    	System.out.println();
    	for (Tuple2<?, ?> tuple : output) {
    	    System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
    	}
    	System.out.println();
        }

    public static void main(String[] args) throws Exception {

	if (args.length < 1) {
	    System.err.println("Usage: JavaWordCount <file or folder>");
	    System.exit(1);
	}

	SparkConf conf = new SparkConf().setAppName("WordCount");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	//JavaRDD<String> lines = sc.textFile(args[0]);
	
	// filename as key and contents as value
	JavaPairRDD<String, String> file = sc.wholeTextFiles(args[0]);
	System.out.println("#partitions: " + file.getNumPartitions());
	//lines = lines.coalesce(4);
	System.out.println("#partitions: " + file.getNumPartitions());
	
	// mapping words to filename
	JavaPairRDD<String,String> lines = file.flatMapToPair(f -> 
	{
		String[] word;
		String[] path = f._1.split("/");
		String filename = path[path.length - 1];
		ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
		word = SPACE.split(f._2);  //get the words in the file
		for(int i = 0; i<word.length;i++)
		{
			list.add(new Tuple2<String, String>(word[i].replace("\n", ""), filename));
			//return new Tuple2<String, String>(f._1, word[i]);
		}
		return list.iterator();
		
	});
	//JavaRDD<String> words = file.(s -> Arrays.asList(SPACE.split(s)).iterator());
	
	//JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
	JavaPairRDD<Tuple2<String, String>, Integer> ones = lines.mapToPair(s -> new Tuple2<>(new Tuple2<String, String>(s._1, s._2), 1));
	JavaPairRDD<Tuple2<String, String>, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
	//JavaPairRDD<String, Integer> filessorted = counts.sortByKey();
	JavaPairRDD<String, Tuple2<Integer,String>> newpair = counts.mapToPair(s -> 
	{
		
		//String[] line = s._1.toString().split("::");
		String word = s._1._1;
		String path = s._1._2;		
		Integer count = s._2;
		//String new_val = count+"::"+path;
		
		return new Tuple2<String, Tuple2<Integer, String>>(word, new Tuple2<Integer, String>(count, path));
	});
	//JavaPairRDD<Integer, String> swapped = counts.mapToPair(s -> s.swap());
	JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> grouped = newpair.groupByKey();
	
	JavaPairRDD<String, String> sorted = grouped.mapToPair(s -> 
	{
		Iterator<Tuple2<Integer, String>> it = s._2.iterator();
		List<String> list = new ArrayList<String>();
	    while (it.hasNext()) {
	        Tuple2<Integer, String> next = it.next();
	        list.add(next._2 + "::" + next._1);
	    }       
	    Collections.sort(list);
	    Map<Integer, List<String>> listCountSorted = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
	    Iterator<String> it2 = list.iterator();
	    while (it2.hasNext()) {
	        String next = it2.next();
	        Integer count = Integer.parseInt(next.split("::")[1]);
        	if (!listCountSorted.containsKey(count))
        		listCountSorted.put(count, new ArrayList<String>());
	        listCountSorted.get(count).add(next.split("::")[0]);
	    }
	    Iterator<Entry<Integer, List<String>>> itrListCountSorted = listCountSorted.entrySet().iterator();
	    List<String> output = new ArrayList<String>();
	    while (itrListCountSorted.hasNext()) {
	    	Entry<Integer, List<String>> next = itrListCountSorted.next();
	    	List<String> files = next.getValue();
	    	for (String str : files) {
	    		output.add(next.getKey() + "::" + str);
	    	}
	    }
		return new Tuple2<String, String>(s._1.toString(), output.toString());
	});
	/*
	JavaPairRDD<String, Tuple2<String,String>> sorted2 = sorted.flatMapToPair(s -> 
	{
		String[] val = s._2.replace("[", "").replace("]", "").split(",");
		Iterator<String> it = Arrays.asList(val).iterator();
		//List list = new ArrayList();
		ArrayList<Tuple2<String, Tuple2<String, String>>> tuplelist = new ArrayList<Tuple2<String, Tuple2<String, String>>>();
	      while (it.hasNext()) {
	    	  String[] tokens = it.next().split("::");
	    	  //int count = Integer.parseInt(val[0]);
	    	  if(tokens.length==2)
	    	  {
	    	  tuplelist.add(new Tuple2<String, Tuple2<String, String>>(s._1, new Tuple2<String, String>(tokens[0], tokens[1])));
	    	  }
	      }
	     
	      return tuplelist.iterator();
	});
	
	
	JavaPairRDD<String, Iterable<Tuple2<String, String>>> grouped2 = sorted2.mapToPair(s->
	{
		JavaRDD<Iterable<String>> asdf = s._2.
	});
			
	//JavaPairRDD<String, Iterable<String>> ordered = (JavaPairRDD<String, Iterable<String>>) grouped.takeOrdered(10);
	//JavaPairRDD<Iterable<String>, String> swapped = grouped.mapToPair(s -> s.swap());//.sortByKey(true);
	
	*/
	
	List<Tuple2<String, String>> output = sorted.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
		System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
	//swapped = swapped.sortByKey(false);
	//counts.saveAsTextFile("hdfs://bugs.boisestate.edu:9000/user/amit/output");

	/*List<Tuple2<String, Integer>> output = counts.collect();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println(tuple._1() + ": " + tuple._2());
	}*/
	sc.stop();
	sc.close();
    }
}

