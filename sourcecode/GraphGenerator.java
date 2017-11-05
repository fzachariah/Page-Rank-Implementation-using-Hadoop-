//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment 3 -Fall 2017

package com.cloud.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

/**
 * @author Febin Zachariah
 * @since 10/23/2017
 * @description GraphGenerator Program performs the MapReduce Job to parse each
 *              line and identify the outgoing links from each page and assign
 *              each page with an initial page rank value of 1/N(obtained from
 *              previous Job).
 * 
 *
 */
public class GraphGenerator extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocumentCount.class);
	private final static String outputPath = "/part-r-00000";
	private final static String seperator = "%#####%";
	static long pageCount;

	/**
	 * @description This method read the count of total pages from the output
	 *              location and is returned to the caller method in order to
	 *              set this value in the configuration.
	 *
	 */
	private long getPageCount(Path countOutput) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fileSystem = countOutput.getFileSystem(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fileSystem.open(countOutput)));
		String line;
		line = br.readLine();
		if (line != null && line.contains("TotalCount")) {
			line = line.replace("TotalCount", "").trim();
			long count = Long.parseLong(line);
			return count;

		}

		return 0;
	}

	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */
	@Override
	public int run(String[] args) throws Exception {

		Path countOutput = new Path(args[1] + outputPath);
		pageCount = getPageCount(countOutput);
		LOG.info("^^^2222 Test: " + pageCount);
		Configuration conf = new Configuration();
		conf.set("Count", String.valueOf(pageCount));

		FileSystem fileSystem = FileSystem.get(conf);
		Path deletePth = new Path(args[1]);
		if (fileSystem.exists(deletePth)) {
			LOG.info("^^^2222 Test: " + pageCount);
			fileSystem.delete(deletePth, true);
		}

		Job job = Job.getInstance(conf, "graphGenerator");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(Map.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * @description The Map Class transforms the input data into intermediate
	 *              key/value pairs and is passed into Reducer class for further
	 *              operations. Here Each line is parsed to extract title and
	 *              corresponding outlinks for each page.
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern link = Pattern.compile("\\[\\[.*?]\\]");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			LOG.info("Entering Mapper Class Link Graph");
			StringBuilder builder = new StringBuilder();
			String pageLine = lineText.toString();
			int startTitle = pageLine.indexOf("<title>");
			int endTitle = pageLine.indexOf("</title>", startTitle);
			String title = pageLine.substring(startTitle + 7, endTitle).trim();
			String textLine = pageLine.replaceAll("<text.*?>", "<text>");
			int startText = textLine.indexOf("<text>");
			if (textLine.contains("</text>")) {
				int endText = textLine.indexOf("</text>");
				Log.info("printing here " + textLine);
				String text = textLine.substring(startText + 6, endText).trim();
				Matcher matcher = link.matcher(text);
				while (matcher.find()) {
					String link = matcher.group().replace("[[", "")
							.replace("]]", "");
					if (link == null || link.length() < 1) {
						continue;
					}
					builder.append(link + seperator);
				}
			}
			context.write(new Text(title), new Text(builder.toString().trim()));

		}
	}

	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output.Intial PageRank value obtained from
	 *              configuration is assigned to each page and creates initial
	 *              link graph
	 *
	 */

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			LOG.info("Entering Reducer Class");
			Configuration configuration = context.getConfiguration();
			long totalPages = Long.parseLong(configuration.get("Count")
					.toString());
			double initialPageRank = (1 / (double) totalPages);

			for (Text line : values) {
				String lineText = line.toString();
				lineText = lineText + initialPageRank;
				context.write(new Text(key), new Text(lineText));
			}

		}
	}

}
