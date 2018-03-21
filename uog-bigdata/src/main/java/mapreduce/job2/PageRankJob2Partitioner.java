package mapreduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import mapreduce.PageRank;

public class PageRankJob2Partitioner extends Partitioner<Text, Text> {
	@Override
     	/*
     	 * This partitioner ensures that each set of the same natural key
     	 * will be processed by a single reducer.
	 * composite key: <article_title¬0>
	 * natural key:   <article_title>
      	 */
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		String[] keyItems = key.toString().split(PageRank.DELIMITER);
		String compKey = keyItems[0];
		
		return (compKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}
