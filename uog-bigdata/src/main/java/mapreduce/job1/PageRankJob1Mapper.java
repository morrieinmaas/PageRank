package mapreduce.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.PageRank;

public class PageRankJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	static enum Counters { NUM_RECORDS, NUM_LINES, NUM_BYTES }
	
	private Text _key = new Text();
	private Text _value = new Text();
		
	/*
	 * This mapper is used to filter out each revision record returned by PageRankJob1RecordReader.
	 * Basically, the key is set to be the article title (i.e. assumming that it is unique) and
	 * the value is the revision id along with the entire MAIN line separated by a single whitespace character.
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] lines = value.toString().split("\n");
		String[] revLine = lines[0].toString().split("\\s");
		
		_key.set(revLine[3].trim());
		_value.set(revLine[2].trim() + " " + lines[3].trim());
		
		context.write(_key, _value);
		context.getCounter(Counters.NUM_LINES).increment(lines.length);
		context.getCounter(Counters.NUM_BYTES).increment(_value.getLength());
		context.getCounter(Counters.NUM_RECORDS).increment(1);
	}
}
