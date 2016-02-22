package com.hillmantech.assignment1.count.sentence.word.frequent.most;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChainMapReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private int maxSum = 0;
	private Text maxKey = new Text();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		System.out.println(key +":" + sum);
		if (sum > maxSum) {
			maxSum = sum;
			maxKey = new Text(key.toString());
//			System.out.println("Max: " + maxKey+":"+maxSum);
			
		}
//		System.out.println(key+":"+sum +"-"+maxKey+":"+maxSum);
//		context.write(key, new IntWritable(sum));
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
//		System.out.println("Max2: " + maxKey+":"+maxSum);
		context.write(maxKey, new IntWritable(maxSum));
		super.cleanup(context);
	}
	
}