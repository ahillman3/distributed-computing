package com.hillmantech.assignment1.count.sentence.end.word.frequent.most;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class SplitMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text sentenceEnd = new Text("Sentence End");
	
    @Override
    protected void map(LongWritable key, Text value, Context context)
                                 throws IOException, InterruptedException {
    	String inputLine = value.toString();
    	List<String> endSentenceChars = new ArrayList<>(Arrays.asList("?","!","."));
    	
		inputLine = inputLine.replace("--", " ").replace("“", "")
				.replace("”", "").replace("\"", "").replace("‘", "").replace("’", "").replace("'", "")
				.replace("-", "").replace(",", "").replace("#", "").toLowerCase().trim();
    	StringTokenizer tokenizer = new StringTokenizer(inputLine);
                   IntWritable dummyValue = new IntWritable(1);
                   while (tokenizer.hasMoreElements()) {
                	   String content = (String) tokenizer.nextElement();
                	   
                	   if (content.length() > 1 && endSentenceChars.contains(
                			   content.substring(content.length() - 1, content.length()))) {
                		   context.write(sentenceEnd, new Text(content.substring(0, content.length() - 1)));
                	   }
                   }
    }
}
//public class SplitMapper extends Mapper<LongWritable, Text, Text, String> {
//	private Text sentenceEnd = new Text("Sentence End");
//	
//	@Override
//	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//		System.out.println(value.toString());
//		String inputLine = value.toString();
//		inputLine = inputLine.replace("--", " ").replace("“", "")
//				.replace("”", "").replace("\"", "").replace("‘", "").replace("’", "").replace("'", "")
//				.replace("-", "").replace(",", "").replace("#", "").toLowerCase().trim();
//		StringTokenizer tokenizer = new StringTokenizer(inputLine);
//		while (tokenizer.hasMoreElements()) {
//			
//			String content = (String) tokenizer.nextElement();
//			context.write(sentenceEnd, content);
//		}
//	}
//}