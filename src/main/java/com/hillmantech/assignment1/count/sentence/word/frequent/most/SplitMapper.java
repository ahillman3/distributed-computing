package com.hillmantech.assignment1.count.sentence.word.frequent.most;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString();

		inputLine = inputLine.replace("--", " ").replace("“", "").replace("”", "").replace("\"", "").replace("‘", "")
				.replace("’", "").replace("'", "").replace("-", "").replace(",", "").replace("#", "")
				.replace("!", "").replace("?", "").replace(".", "")
				.toLowerCase()
				.trim();
		StringTokenizer tokenizer = new StringTokenizer(inputLine);
		IntWritable dummyValue = new IntWritable(1);
		while (tokenizer.hasMoreElements()) {
			String content = (String) tokenizer.nextElement();
			context.write(new Text(content), dummyValue);
		}
	}
}