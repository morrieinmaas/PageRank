package mapreduce.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.PageRank;

public class PageRankJob3Mapper extends Mapper<Text, Text, Text, Text> {
	
	static enum Counters { NUM_OF_DEL_WITH_0, NUM_OF_DEL_WITH_1 }

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.toString().contains(PageRank.DELIMITER + "0"))
			context.getCounter(Counters.NUM_OF_DEL_WITH_0).increment(1);
		else if(key.toString().contains(PageRank.DELIMITER + "1"))
			context.getCounter(Counters.NUM_OF_DEL_WITH_1).increment(1);
		
		context.write(key, value);
	}
}
