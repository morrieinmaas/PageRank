package mapreduce.job4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.PageRank;

public class PageRankJob4Mapper extends Mapper<Text, Text, Text, Text> {
	
	static enum Counters { NUM_PRs_CALCULATED, NUM_CONTRIBUTORS }
	
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.toString().contains(PageRank.DELIMITER + "0"))
			context.getCounter(Counters.NUM_PRs_CALCULATED).increment(1);
		else if(key.toString().contains(PageRank.DELIMITER + "1"))
			context.getCounter(Counters.NUM_CONTRIBUTORS).increment(1);
		
		context.write(key, value);
	}

}
