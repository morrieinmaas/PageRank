package mapreduce.job2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.PageRank;


public class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
	
	static enum Counters { NUM_OF_ARTICLES, NUM_OF_OUTLINKS, NUM_OF_DELIMITER_1, NUM_OF_FIRST }
	private Text _key = new Text();
	private Text _value = new Text();
	/*
	 *  Initial value used for the first iteration for the Job#3 Page Ranking,
	 *  as all articles should be initialized to 0.
	 *
	 *  Input pair:  <article_title¬0>	<total_#_of_outlinks>
	 *  			 <article_title¬1>	<out-link>
	 * 
	 *  Output pair: <outlink>		<article_title¬1¬total_#_of_outlinks>
	 * 		 
	 */
	private final static IntWritable one = new IntWritable(1);
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		String[] keyItems = key.toString().split(PageRank.DELIMITER);
		String numOutlinks = "";
		boolean first = true;
				
		while(it.hasNext()) {

			String _val = it.next().toString();
						
			if(first) {
				numOutlinks = _val;
				first = false;
				continue;
			}
			else {
				_key.set(_val);
				_value.set(keyItems[0] + PageRank.DELIMITER + one + PageRank.DELIMITER + numOutlinks);
				
				context.write(_key, _value);
				context.getCounter(Counters.NUM_OF_OUTLINKS).increment(1);
			}
		}
		context.getCounter(Counters.NUM_OF_ARTICLES).increment(1);
	}	
}
