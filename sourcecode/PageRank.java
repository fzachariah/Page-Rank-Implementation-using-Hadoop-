//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment 3 -Fall 2017

package com.cloud.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * @author Febin Zachariah
 * @since 10/24/2017
 * @description PageRank Program performs the MapReduce Job to calculate new
 *              PageRank values from existing page rank values by using the page
 *              rank formula.This MapReduce job is called iteratively from the
 *              driver program
 * 
 *
 */
public class PageRank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(PageRank.class);
	private final static String SEPERATOR = "%#####%";
	private final static String SEPERATOR1 = "%%%%%";
	private final static String PAGE_IDENTIFIER = "*valid*";
	private final static String NONPAGE_IDENTIFIER = "*invalid*";

	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "pagerank");
		job.setJarByClass(this.getClass());
		LOG.info("^^^^^^^3" + args[0]);
		LOG.info("^^^^^^^4" + args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		int result = job.waitForCompletion(true) ? 0 : 1;

		FileSystem fileSystem = FileSystem.get(getConf());
		Path deletePth = new Path(args[0]);
		if (fileSystem.exists(deletePth)) {
			fileSystem.delete(deletePth, true);
		}

		return result;
	}

	/**
	 * @description The Map Class transforms the input data into intermediate
	 *              key/value pairs and is passed into Reducer class for further
	 *              operations. Here, Each line is parsed to extract
	 *              title,corresponding outlinks, and pagerank value for each
	 *              page.Used seperartors from identiying actual pages and the
	 *              parsed pagerank is distributed among outlinks of the page.
	 * 
	 *
	 */
	public static class Map extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text lineText, Context context)
				throws IOException, InterruptedException {

			LOG.info("Entering Mapper Class Page Rank");

			String pageTitle = key.toString();
			LOG.info("Entering Mapper Class Page Rank" + lineText.toString());
			String line = lineText.toString();
			String[] splitterArray = line.split(SEPERATOR);

			// links ranging from 0 to n-2
			int linksCount = splitterArray.length - 1;

			if (linksCount == 0) {
				context.write(new Text(pageTitle), new Text(PAGE_IDENTIFIER
						+ SEPERATOR1));

			}

			LOG.info("Count: " + linksCount + " Line" + line);
			double pageRank = Double
					.parseDouble(splitterArray[splitterArray.length - 1]);
			double splittedRank = pageRank / linksCount;

			for (int i = 0; i < splitterArray.length - 1; i++) {
				context.write(new Text(splitterArray[i]), new Text(
						NONPAGE_IDENTIFIER + SEPERATOR + splittedRank));
			}
			context.write(new Text(pageTitle), new Text(PAGE_IDENTIFIER
					+ SEPERATOR1 + line));

		}
	}

	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output.Here, we are calculating the page rank
	 *              for each page by using damping factor and also Reducer is
	 *              created in such a way that it will write the ouptut in the
	 *              same format the abover Mapper class gets its input.
	 *
	 */

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private double dampingFactor = 0.85;

		@Override
		public void reduce(Text page, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			double sum = 0.0;
			boolean required = false;
			String mainLine = "";

			StringBuilder actualResult = new StringBuilder("");

			for (Text line : values) {
				String lineText = line.toString();
				if (lineText.contains(NONPAGE_IDENTIFIER)) {
					LOG.info("Entering Reducer Class Page Rank" + lineText);
					String rankArray[] = lineText.split(SEPERATOR);
					double rank = Double.parseDouble(rankArray[1]);
					sum = sum + rank;
				} else if (lineText.contains(PAGE_IDENTIFIER)) {
					required = true;
					if (lineText.contains(SEPERATOR1)) {
						LOG.info("Entering Reducer Class Page Rank" + lineText);
						if (lineText.contains(SEPERATOR)) {
							mainLine = lineText.split(SEPERATOR1)[1];
							String array[] = mainLine.split(SEPERATOR);
							for (int i = 0; i < array.length - 1; i++) {
								actualResult.append(array[i]);
								actualResult.append(SEPERATOR);
							}
						}
					}
				}
			}
			if (required) {
				double pageRank = (1 - dampingFactor) + (dampingFactor * sum);
				String result = actualResult.toString();
				result = result + pageRank;
				context.write(new Text(page), new Text(result));

			}

		}
	}

}
