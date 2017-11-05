//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment 3 -Fall 2017

package com.cloud.pagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * @author Febin Zachariah
 * @since 10/23/2017
 * @description DocumentCount Program performs the MapReduce Job to calculate
 *              the total number of pages present.
 *
 */

public class DocumentCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocumentCount.class);

	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "documentcount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @description The Map Class transforms the input data into intermediate
	 *              key/value pairs and is passed into Reducer class for further
	 *              operations. Here Each line is parsed and and value is set to
	 *              one for each line.
	 *
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			LOG.info("Entering Mapper Class");
			String line = lineText.toString();

			if (line != null && line.length() > 0) {
				context.write(new Text("Page"), one);
			}

		}
	}

	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output.Total number of pages are calculated
	 *              here.
	 *
	 */

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			LOG.info("Entering Reducer Class");
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(new Text("TotalCount"), new IntWritable(sum));
		}
	}

}
