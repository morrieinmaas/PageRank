package mapreduce.job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.PageRank;

public class PageRankJob3aReducer extends Reducer<Text, Text, Text, Text> {
	
	enum Counters{ NUM_INVALID_NUMBER_FORMAT }
	
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
				context.getCounter(Counters.NUM_INVALID_NUMBER_FORMAT).increment(1);
				context.write(new Text("#" + key), value);
				break;
			}
			sumPageRank += (pageRank / totalLinks);
		}
		newPageRank = PageRank.OFFSET + PageRank.DAMPING * sumPageRank;
		
		context.write(new Text(keyItems[0]), new Text(newPageRank + ""));
	}
}