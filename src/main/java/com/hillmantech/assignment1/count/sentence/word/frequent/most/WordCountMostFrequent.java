package com.hillmantech.assignment1.count.sentence.word.frequent.most;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * 
 * @author ahillman
 *
 */
public class WordCountMostFrequent {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.print("Useage: WordCountMostFrequent <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance();
		Configuration splitMapConfig = new Configuration(false);
		ChainMapper.addMapper(job, SplitMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class,
				splitMapConfig);
		job.setJarByClass(WordCountMostFrequent.class);
		job.setCombinerClass(ChainMapReducer.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ChainMapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path outputPath = new Path(otherArgs[1]);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(config).delete(outputPath, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}