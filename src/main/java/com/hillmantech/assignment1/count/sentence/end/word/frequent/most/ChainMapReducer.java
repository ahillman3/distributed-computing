package com.hillmantech.assignment1.count.sentence.end.word.frequent.most;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ChainMapReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private MultipleOutputs<Text, IntWritable> output;
    
	  @Override  
	  public void setup(Context context)
	  {
	      output = new MultipleOutputs<>(context);
	  }
	  
	
	private int maxSum = 0;
	private Text maxKey = new Text();
	private int totalEndingWords = 0;
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
			System.out.println("Max: " + maxKey+":"+maxSum);
			
		}
//		Another sentence ending word, increment.
		totalEndingWords++;
//		System.out.println(key+":"+sum +"-"+maxKey+":"+maxSum);
//		context.write(key, new IntWritable(sum));
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
//		System.out.println("Max2: " + maxKey+":"+maxSum);
		output.write("maxcount", maxKey, new IntWritable(maxSum));
//		context.write(maxKey, new IntWritable(maxSum));
		output.write("wordcount", new Text("TotalSentenceEndingWords"), new IntWritable(totalEndingWords));
//		context.write(new Text("TotalSentenceEndingWords"), new IntWritable(totalEndingWords));
		output.close();
		super.cleanup(context);
	}
	
}