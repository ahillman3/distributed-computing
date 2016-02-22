package com.hillmantech.assignment1.count.vowels;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat; 
/**
 * Simple hadoop example to count the number of each of the 5 vowels.</br>
 * Input to the program are an input path and output path.</br>
 * Output provided in the /com/hillmantech/assignment1/count/vowels/output directory.
 * @author ahillman
 *
 */
public class VowelCount {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text vowel_a = new Text("Vowel 'a'");
		private Text vowel_e = new Text("Vowel 'e'");
		private Text vowel_i = new Text("Vowel 'i'");
		private Text vowel_o = new Text("Vowel 'o'");
		private Text vowel_u = new Text("Vowel 'u'");

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String inputLine = value.toString();
			// Clean up all the characters we don't care about.
			inputLine = inputLine.replace("--", " ").replace("?", "").replace(".", "").replace("!", "").replace("“", "")
					.replace("”", "").replace("\"", "").replace("‘", "").replace("’", "").replace("'", "")
					.replace("-", "").replace(",", "").replace("#", "").toLowerCase().trim();
			String tmpChar = "";
			for (int i = 0; i < inputLine.length(); i++) {
				tmpChar = inputLine.substring(i, i + 1);
				switch (tmpChar) {
				case "a":
					output.collect(vowel_a, one);
					break;
				case "e":
					output.collect(vowel_e, one);
					break;
				case "i":
					output.collect(vowel_i, one);
					break;
				case "o":
					output.collect(vowel_o, one);
					break;
				case "u":
					output.collect(vowel_u, one);
					break;
				default:
					break;
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(VowelCount.class);
		conf.setJobName("VowelCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}