package com.hillmantech.assignment1.count.sentence.end.word.frequent.most;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class EndWordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

              @Override
              protected void map(Text key, Text value, Context context)
                                           throws IOException, InterruptedException {
                             String val = key.toString().toLowerCase();
                             IntWritable dummyValue = new IntWritable(1);
                             Text newKey = new Text(val);
                             context.write(value, dummyValue);
              }
}
//import java.io.IOException;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//public class LowerCaseMapper extends Mapper<Text, String, Text, String> {
//
//	@Override
//	protected void map(Text key, String value, Context context) throws IOException, InterruptedException {
//		String val = key.toString().toLowerCase();
//		Text newKey = new Text(val);
//		context.write(newKey, value);
//	}
//}