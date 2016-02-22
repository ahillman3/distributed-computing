package com.hillmantech.assignment1.count.sentence.end.word.frequent.most;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * This program is a departure from some of the previous examples.</br>
 * Newer client classes were provided, making many of the goals easier to achieve.</br>
 * The item of note in here is the use of global counters for the max word count and the word itself.</br>
 * Additionally the configuration was told that there was only one reducer, so it would get all of the data.</br>
 * Added another counter for total sentence ending words, since we are getting them all.</br>
 * The final values are written during the cleanup of the reducer.<p>
 * Had issues with trying to write different data to the same output file, so I split up the output to two files.
 * @author ahillman
 *
 */
public class EndingWordCountMostFrequent {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.print("Useage: EndingWordCountMostFrequent <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance();
		Configuration splitMapConfig = new Configuration(false);
		ChainMapper.addMapper(job, SplitMapper.class, LongWritable.class, Text.class, Text.class, Text.class,
				splitMapConfig);
		Configuration lowerCaseMapConfig = new Configuration(false);
		ChainMapper.addMapper(job, EndWordCountMapper.class, Text.class, Text.class, Text.class, IntWritable.class,
				lowerCaseMapConfig);
		job.setJarByClass(EndingWordCountMostFrequent.class);
		job.setCombinerClass(ChainMapReducer.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ChainMapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path outputPath = new Path(otherArgs[1]);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(config).delete(outputPath,true);
		 MultipleOutputs.addNamedOutput(job, "wordcount", TextOutputFormat.class, Text.class, IntWritable.class );
         MultipleOutputs.addNamedOutput(job, "maxcount", TextOutputFormat.class, Text.class, IntWritable.class );

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}