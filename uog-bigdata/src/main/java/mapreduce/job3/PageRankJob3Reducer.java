package mapreduce.job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.PageRank;

public class PageRankJob3Reducer extends Reducer<Text, Text, Text, Text> {
	
	enum Counters{ NUM_OF_DELIMETER_1, NUM_OF_DELIMETER_0, NUM_INVALID_NUMBER_FORMAT }
	private final static IntWritable one = new IntWritable(1);
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
																				   InterruptedException {
		
		Iterator<Text> it = values.iterator();
		String[] keyItems = key.toString().split(PageRank.DELIMITER);
		double sumPageRank = 0.0;
		double newPageRank = 0.0;
		
		while(it.hasNext()) {

			Text value = it.next();
			
			String[] valueItems = value.toString().split(PageRank.DELIMITER);
			String inLink = "";
			double pageRank = 0.0;
			int totalLinks = 0;
						
			try {
				inLink = valueItems[0];
				pageRank = Double.parseDouble(valueItems[1]);
				totalLinks = Integer.parseInt(valueItems[2]);
			}
			catch(NumberFormatException e) {
				context.write(new Text("#" + key), value);
				context.getCounter(Counters.NUM_INVALID_NUMBER_FORMAT).increment(1);
			}
			
			sumPageRank += (pageRank / totalLinks);
						
			context.write(new Text(inLink + PageRank.DELIMITER + "1"), new Text("1" + PageRank.DELIMITER + keyItems[0] + PageRank.DELIMITER + one + PageRank.DELIMITER + totalLinks));
			context.getCounter(Counters.NUM_OF_DELIMETER_1).increment(1);
		}
		newPageRank = PageRank.OFFSET + PageRank.DAMPING * sumPageRank;
		
		context.write(new Text(key + PageRank.DELIMITER + "0"), new Text("0" + PageRank.DELIMITER + newPageRank + ""));
		context.getCounter(Counters.NUM_OF_DELIMETER_0).increment(1);
	}
}