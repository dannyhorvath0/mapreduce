package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(WordCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s");

		for (String number : tokens) {
			int a = divisors(Integer.parseInt(number));
			context.write(new IntWritable(Integer.parseInt(number)), new IntWritable(a));
		}
	}

	public int divisors(int n){
		int sum = 0;
		for(int i = 1; i < (n/2+1); i++){
			if(n % i == 0){
				sum += i;
			}
		}
		return sum;
	}
}

class WordCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	HashMap<Integer, Integer> myNumbers = new HashMap<Integer, Integer>();
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for (IntWritable value : values) {
			if (myNumbers.containsKey(Integer.parseInt(value.toString()))) {
				if(Integer.parseInt(key.toString()) == myNumbers.get(Integer.parseInt(value.toString()))){
					context.write(value, key);
				}
			} else {
				myNumbers.put(Integer.parseInt(key.toString()), Integer.parseInt(value.toString()));
			}
		}
	}
}