//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment 3 -Fall 2017

package com.cloud.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * @author Febin Zachariah
 * @since 10/28/2017
 * @description PageRankSort Program performs the MapReduce Job to sort the
 *              pages in descending order of pagerank values by using a subclass
 *              of WritableComparator.
 *
 */
public class PageRankSort extends Configured implements Tool {

	private final static String seperator = "%#####%";
	private static final Logger LOG = Logger.getLogger(PageRankSort.class);

	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "pagerankSort");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setSortComparatorClass(PageSortComparator.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(1);
		int result = job.waitForCompletion(true) ? 0 : 1;

		FileSystem fileSystem = FileSystem.get(getConf());
		Path deletePth = new Path(args[2]);
		if (fileSystem.exists(deletePth)) {
			fileSystem.delete(deletePth, true);
		}
		return result;
	}

	/**
	 * @description The Map Class transforms the input data into intermediate
	 *              key/value pairs and is passed into Reducer class for further
	 *              operations. *
	 */
	public static class Map extends Mapper<Text, Text, DoubleWritable, Text> {

		public void map(Text key, Text lineText, Context context)
				throws IOException, InterruptedException {

			LOG.info("Entering Map Class PageRank Sorting");
			String line = lineText.toString();
			String[] splitterArray = line.split(seperator);
			double rank = Double
					.parseDouble(splitterArray[splitterArray.length - 1]);
			context.write(new DoubleWritable(rank), key);

		}
	}

	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output.
	 *
	 */

	public static class Reduce extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {

			LOG.info("Entering Reduce Class");
			for (Text line : counts) {

				LOG.info("Entering Reduce Test" + line);
				context.write(line, word);
			}

		}
	}

}

/**
 * @description The Comparator class for sorting the pages in descening order
 *              based on the pagerank values.
 *
 */
class PageSortComparator extends WritableComparator {

	protected PageSortComparator() {
		super(DoubleWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		DoubleWritable k1 = (DoubleWritable) o1;
		DoubleWritable k2 = (DoubleWritable) o2;
		int cmp = k1.compareTo(k2);
		return -1 * cmp;
	}
}
