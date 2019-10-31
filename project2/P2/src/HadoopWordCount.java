
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

public class HadoopWordCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    class Map1 extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, OutputCollector<IntWritable, Text> collector, Reporter arg3) throws IOException {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            {
                int number = 999;
                String word = "empty";

                if (stringTokenizer.hasMoreTokens()) {
                    String str0 = stringTokenizer.nextToken();
                    word = str0.trim();
                }

                if (stringTokenizer.hasMoreElements()) {
                    String str1 = stringTokenizer.nextToken();
                    number = Integer.parseInt(str1.trim());
                }
                collector.collect(new IntWritable(number), new Text(word));
            }

        }

    }

    class Reduce1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> arg2, Reporter arg3) throws IOException {
            while ((values.hasNext())) {
                arg2.collect(key, values.next());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(HadoopWordCount.class);
        conf.setJobName("wordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path("/tmp/temp"));

    //JobClient.runJob(conf);
        //------------------------------------------------------------------
        JobConf conf2 = new JobConf(HadoopWordCount.class);
        conf2.setJobName("WordCount1");

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(IntWritable.class);

        conf2.setMapperClass(Map1.class);
        conf2.setCombinerClass(Reduce1.class);
        conf2.setReducerClass(Reduce1.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, new Path("/tmp/temp/part-00000"));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

        Job job1 = new Job(conf);
        Job job2 = new Job(conf2);

        job1.submit();
        if (job1.waitForCompletion(true)) {
            job2.submit();
            job2.waitForCompletion(true);
        }
        }

    }
