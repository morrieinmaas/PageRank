package mapreduce.job4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.PageRank;

public class PageRankJob4Reducer extends Reducer <Text, Text, Text, Text> {
	
	static enum Counters { NUM_WITH_INLINKS_AND_WITHOUT_OUTLINKS, NUM_ARRAY_OUT_OF_BOUNDS, NUM_ARRAY_OUT_OF_BOUNDS_2 }
	
	private Text _key = new Text();
	private Text _value = new Text();
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		String newPageRank = "";
		boolean first = true;
		
		while(it.hasNext()) {
			/*
			 * Key-value format:
			 *      K		  		V
			 *  1. articleTitle¬0	0¬pageRank
			 *  2. articleTitle¬1	1¬inLink¬pageRank¬#Outlinks
			 *  
			 * Assumptions:
			 *  1. Article with no OUTLINKS: any single pair (i.e. without more than one value in the iterator)with page rank as a value (key: ends with ¬0 and value: starts with¬0)
			 * 	   is skipped since can be computed again with the new PRs.
			 *  2. Article with no INLINKS: multiple pairs (key ends with ¬1 and value starts with ¬1)
			 *     							with many outlinks as values.
			 *  3. Article followed by its inlinks to be updated with a new PR: the first comes first with the PR.
			 */
			Text value = it.next();
			String[] valueItems = value.toString().split(PageRank.DELIMITER);
			String[] keyItems = key.toString().split(PageRank.DELIMITER);
			
			if(first) {
				
				if(!it.hasNext() && valueItems[0].equals("0")) {
					context.getCounter(Counters.NUM_WITH_INLINKS_AND_WITHOUT_OUTLINKS).increment(1);
					break;
				}
				else if(it.hasNext() && valueItems[0].equals("0")) {
					/*
					 * This try/catch statement is used to handle erroneous pairs during the debugging.
					 * In such case the particular pair denoted by the # at the beginning of the key.
					 */
					try{
						newPageRank = valueItems[1];
					}
					catch(ArrayIndexOutOfBoundsException e) {
						context.write(new Text("#" + key), value);
						context.getCounter(Counters.NUM_ARRAY_OUT_OF_BOUNDS).increment(1);
						continue;
					}
					first = false;
					continue;
				}
				else if(valueItems[0].equals("1")) {
					newPageRank = valueItems[2];
				}
			}
			
			try {
				_value.set(new Text(keyItems[0] + PageRank.DELIMITER + newPageRank + PageRank.DELIMITER + valueItems[3]));
				_key.set(valueItems[1]);
				context.write(_key, _value);
			}
			catch(ArrayIndexOutOfBoundsException e){
				context.write(new Text("#2" + key), value);
				context.getCounter(Counters.NUM_ARRAY_OUT_OF_BOUNDS_2).increment(1);
			}
		}
	}
}