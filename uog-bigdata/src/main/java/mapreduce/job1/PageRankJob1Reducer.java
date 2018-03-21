package mapreduce.job1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.PageRank;

public class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {
	
	private Text _value = new Text();
	
	/*
	 * The reducer is used to determined the most recent article based on the revision id.
	 * Assumption: the bigger the number of revision id the most recent the revision of the article.
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int max = 0;
		
		Iterator<Text> it = values.iterator();
		Text current =new Text();
		
		while(it.hasNext()) {
			current = it.next();
			String[] value =  current.toString().split("\\s");
			
			int revisionId = Integer.parseInt(value[0]);
			
			if(revisionId > max) {
				max = revisionId;
				_value.set(current);
			}
		}
		context.write(key, _value);
	}
	
}
