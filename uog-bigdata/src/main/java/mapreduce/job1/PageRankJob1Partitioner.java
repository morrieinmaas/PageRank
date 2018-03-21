package mapreduce.job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import mapreduce.PageRank;

public class PageRankJob1Partitioner extends Partitioner<Text, Text> {
	public int getPartition(Text key, Text value, int numPartitions) {
		
		int c = Character.toLowerCase(key.toString().charAt(0));
		
		if (c < 'a' || c > 'z')
			return numPartitions - 1;
		
		return (int)Math.floor((float)(numPartitions - 2) * (c-'a')/('z'-'a'));
	}
}