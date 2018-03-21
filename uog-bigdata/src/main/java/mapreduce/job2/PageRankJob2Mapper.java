package mapreduce.job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.PageRank;


public class PageRankJob2Mapper extends Mapper<Text, Text, Text, Text> {
	
	static enum Counters { NUM_OF_ARTICLES, NUM_OF_OUTLINKS, NUM_ARTICLES_WITHOUT_OUTLINKS, NUM_OF_SELF_OUTLINKS_SKIPPED, NUM_ARTICLES_WITHOUT_REVID_OR_MAIN_TAG }
	
	/*
	 * Input pair:	<article_title>		<revision_id MAIN_line>
	 * 
	 * Output pair:	<article_title¬0>	<number_of_outlinks_without_self-outlinks>
	 * 		<article_title¬1>	<outlink>
	 */
	protected void map(Text key, Text value, Context context) throws IOException,
																	 InterruptedException {
		/*
		 * Assumption: to avoid any misleading whitespace used to separate
		 * the outlinks in the MAIN line, the regex for any whitespace character
		 * is used to split the outlinks.
		 */
		String[] outlinks = value.toString().split("\\s");
		String articleTitle = key.toString();
	
		/*
		 * Assumption: since the first two terms (i.e. revision_Id, MAIN tag) <value> it is skipped in the for loop,
		 *	       by starting at the third location of the array. 
		 */
		if(outlinks.length > 2) {
			
			int selfOutlinks = 0;
			/*
			 * Assumption: any article does not include itself in the outlinks.
			 * 	       any such self-outlink is skip and it is not included in its total
			 *	       number of the outlinks.	
			 */
			for(int i = 2; i < outlinks.length; i++) {
				if(!outlinks[i].equals(articleTitle)) {
					context.write(new Text(articleTitle + PageRank.DELIMITER + "1"), new Text(outlinks[i].trim() + ""));
					context.getCounter(Counters.NUM_OF_OUTLINKS).increment(1);
				}
				else {
					context.getCounter(Counters.NUM_OF_SELF_OUTLINKS_SKIPPED).increment(1);
					selfOutlinks += 1;
				}
			}
			
			context.write(new Text(articleTitle + PageRank.DELIMITER + "0"),
					new Text(((outlinks.length - selfOutlinks) - 2) + ""));
			context.getCounter(Counters.NUM_OF_ARTICLES).increment(1);
		}
		else {
			/*
			 * Assumption: skip any article without out-links since the PageRank algorithm
			 * 				 only consider the # of out-links of each in-link of each article.
			 * 				 Hence, the PR for any such article will compute based on the out-links
			 * 			 	 of its in-links.
			 */
			if(outlinks.length < 2)
				context.getCounter(Counters.NUM_ARTICLES_WITHOUT_REVID_OR_MAIN_TAG).increment(1);
			else {
				context.getCounter(Counters.NUM_ARTICLES_WITHOUT_OUTLINKS).increment(1);
				context.write(new Text(articleTitle + PageRank.DELIMITER + "0"), new Text(0 + ""));
			}
		}
	}
}
