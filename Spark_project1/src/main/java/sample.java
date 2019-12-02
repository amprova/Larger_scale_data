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
import org.apache.spark.broadcast.Broadcast;
import org.spark_project.guava.collect.Lists;

import java.util.regex.Pattern;
import java.util.*; 

public class sample
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
	// filename as key and contents as value
	JavaPairRDD<String, String> file = sc.wholeTextFiles(args[0]);
	System.out.println("#partitions: " + file.getNumPartitions());
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
		}
		return list.iterator();
		
	});
	
	JavaPairRDD<String, Integer> ones = lines.mapToPair(s -> new Tuple2<>(s._1 + "::" + s._2, 1));  //((word,filename),1)
	JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);  //((words,filename),n)
	JavaPairRDD<String, Integer> filessorted = counts.sortByKey();					//sorted by filename
	JavaPairRDD<Integer, String> swapped = filessorted.mapToPair(s->s.swap());		//(n, (word,filename)
	JavaPairRDD<Integer, String> sortedcount = swapped.sortByKey(false);			//sorted by count
	JavaPairRDD<String, Tuple2<Integer,String>> newpair = sortedcount.mapToPair(s -> 
	{
		
		String[] line = s._2.toString().split("::");
		
		String path = line[1];
		String word = line[0];
		Integer count = s._1;
		return new Tuple2<String, Tuple2<Integer, String>>(word, new Tuple2<Integer, String>(count, path));
	});  //((word, (count,filename))
	JavaPairRDD<String, Iterable<Tuple2<Integer,String>>> grouped = newpair.groupByKey();  // grouped by word
	List<Tuple2<String, Iterable<Tuple2<Integer,String>>>> output = grouped.collect();
	System.out.println();
	for (Tuple2<?, ?> tuple : output) {
		System.out.println("(" + tuple._1() + "," + tuple._2() + ")");
	}
	System.out.println();
	
	sc.stop();
	sc.close();
    }
}

