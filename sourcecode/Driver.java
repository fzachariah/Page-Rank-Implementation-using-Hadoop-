//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment 3 -Fall 2017

package com.cloud.pagerank;

import org.apache.hadoop.util.ToolRunner;

/**
 * @author Febin Zachariah
 * @since 10/23/2017
 * @description Driver Program initiates all the MapReduce jobs required to
 *              calculate the page rank of each page and rank them in descending
 *              order.
 *
 */
public class Driver {

	/**
	 * @description Entry point of the Program. It calls the 4 Map Reduce jobs
	 *              sequentially. 1) MapReduce job to calculate the number of
	 *              Pages. 2) MapReduce Job to generate the initial Link Graph
	 *              with 1/N as page Rank 3) MapReduce Job to calculate the page
	 *              rank of each iteratively through 10 times. 4) MapReduce Job
	 *              to perform the sorting of pages based on the page rank
	 *              values obtained from the above MapReduce job
	 *
	 */
	public static void main(String[] args) throws Exception {

		int pagerankResult = 1;
		int countRes = ToolRunner.run(new DocumentCount(), args);

		if (countRes == 0) {
			int res = ToolRunner.run(new GraphGenerator(), args);
			if (res == 0) {
				String[] newArgs = new String[2];
				for (int i = 0; i < 10; i++) {

					if (i == 0) {
						newArgs[0] = args[2];
						int temp = i + 1;
						newArgs[1] = args[2] + temp;
					} else if (i == 9) {
						newArgs[0] = newArgs[1];
						newArgs[1] = args[2];
					} else {
						newArgs[0] = newArgs[1];
						int temp = i + 1;
						newArgs[1] = args[2] + temp;
					}
					pagerankResult = ToolRunner.run(new PageRank(), newArgs);
				}
				if (pagerankResult == 0) {

					int sortResult = ToolRunner.run(new PageRankSort(), args);
					System.exit(sortResult);

				}
			}
		}

	}

}
