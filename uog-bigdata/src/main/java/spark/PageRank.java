package spark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Iterables;

import scala.Tuple2;


public class PageRank {
	
	public static void main(String[] args) throws ParseException {
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank"));
		Configuration conf = new Configuration();
		/*
		 * Each revision's record is separated by a double new line delimiter.
		 */
		conf.set("textinputformat.record.delimiter", "\n\n");
		
		String IN_PATH = args[0];
		String OUT_PATH = args[1];
        int ITERATIONS = Integer.parseInt(args[2]);
						
		SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");
        Date IN_DATE = FORMAT.parse(args[3]);
        
        /*
		 * REMEMBER: Since RDDs are immutable it's impossible to modified an existing one!
		 */
		JavaPairRDD<String, Iterable<String>> recordsRDD =
				/*
				 * Since our input file is located in HDFS, the newAPIHadoopFile()
				 * method is used to pass as a parameter a configuration object
				 * with the identical delimiter to separate properly each revision record.
				 * Since this returned a key-value pair the content that is going to be
				 * used is passed as a value.
				 * Hence, the map() is used by passing only the value as s string to the RDD. 
				 */
				sc.newAPIHadoopFile(IN_PATH, TextInputFormat.class, LongWritable.class, Text.class, conf).map(t -> t._2.toString())
				/*
				 * Filter-out any revision with a post-dated creation date D
				 * where D > Y for any valid Y date that the user specified as arg[4].
				 */
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) throws ParseException {	
						return FORMAT.parse(s.split("\n")[0].split(" ")[4].split("T")[0]).before(IN_DATE);
						}
					}
				)
				/*
				 * mapToPair() transformation is used to create a pair
				 * of each record with the following key-value format:
				 * (article_title, revision_id¬MAIN)
				 * This design decision is explained with the use of
				 * the following transformation operation.
				 */
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String s) {
						String[] lines = s.split("\n");
						String[] revisionLine = lines[0].split(" ");
						String mainLine = lines[3];
						return new Tuple2<String, String> (revisionLine[3], revisionLine[2] + "¬" + mainLine);
					}
				})
				/*
				 * reduceByKey() transformation is used to combine all the
				 * available revisions of each article together as a result
				 * to returned the most recent one (i.e. higher revision_id). 
				 */
				.reduceByKey(new Function2<String, String, String>() {
					public String call(String str1, String str2) {
						String[] value1 = str1.split("¬");
						int revisionID1 = Integer.parseInt(value1[0]);
						String[] value2 = str2.split("¬");
						int revisionID2 = Integer.parseInt(value2[0]);
						return (revisionID1 > revisionID2) ? str1 : str2;
					}
				})
				/*
				 * flatMapValues() transformation operation is used to 
				 * tokenize the outlinks of each revision article (i.e. MAIN line).
				 */
				.flatMapValues(new Function<String, Iterable<String>>() {
					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(" "));
					}
				})
				/*
				 * Filter-out pairs with duplicates.
				 */
				.distinct()
				/*
				 * Filter-out self-loops and the pair with the value
				 * (revision_id¬MAIN) used to determine the most recent revision.
				 */
				.filter(new Function<Tuple2<String, String>, Boolean>() {
					public Boolean call(Tuple2<String, String> s) {	
						if(s._1.equals(s._2) || s._2.endsWith("¬MAIN"))
							return false;
						else
							return true;
						}
					}
				)
				/*
				 * Group each article with its out-links.
				 */
				.groupByKey()
				/*
				 * Since these RDD's data is static and is going to be accessed 
				 * each iteration it is very cheap hash-partition 
				 */
				.partitionBy(new HashPartitioner(50))
				/*
				 * The last operation performed on recordsRDD is the cache()
				 * to keep it in RAM since will be used for each iteration. 
				 */
				.cache();
		
		/*
		 * Initialize the rank for each article equal to 1.0.
		 */
		JavaPairRDD<String, Double> ranksRDD = recordsRDD.mapValues(v -> 1.0);
	
		/*
		 * PageRank algorithm.
		 */
		for(int i = 0; i < ITERATIONS; i++) {
		 	JavaPairRDD<String, Double> contributions = recordsRDD
			/*
			 * Join records and ranks RDDs together to get each article
			 * with its outlinks (i.e. using values()) and current rank,
			 * and therefore, using flatMaptoPair() transformation to 
			 * prepare the contribution for each article's neighbors. 
			 */
 			.join(ranksRDD)
		 	.values()
		 	.flatMapToPair(s -> {
		 		int outlinks = Iterables.size(s._1());
		 		List<Tuple2<String, Double>> pageRanks = new ArrayList<>();
		 		for(String article: s._1) {
		 			pageRanks.add(new Tuple2<>(article, s._2() / outlinks));
		 		}
		 		return pageRanks;		 		
		 	}); 	
		 	/*
		 	 * Calculate the page rank of each article by first adding those values 
		 	 * obtained before, and then by multiply these sum with dampbing and offset.
		 	 */
		 	ranksRDD = contributions.reduceByKey((x, y) -> x + y).mapValues(sum -> 0.15 + 0.85 * sum);						
		}
		
		/*
		 * mapToPair() transformation operation is used to swap the
		 * each pair in order to sort them by key.
		 */
		ranksRDD.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
			public Tuple2<Double, String> call(Tuple2<String, Double> pair) {
				return new Tuple2<Double, String>(pair._2 , pair._1);
			}
		})
		/*
		 * sortByKey() transformation operation is used to sort the output
		 * in a descending order based on ranking score of each article.
		 * The parameter value by default is true that implies to ascending order.
		 */
		.sortByKey(false)
		/*
		 * For the same reason here, the use of the mapToPair() operation is for
		 * swapping back the pairs, since the sorting of the scores is done.
		 */
		.mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<Double, String> pair) {
				return new Tuple2<String, Double>(pair._2 , pair._1);
			}
		})
		/*
		 * Prepare the final output format by removing the parentheses and comma of each pair:
		 * 	e.g.	Article_1	score
		 */
		.map(x -> x._1 + " " + x._2)
		/*
		 * Save the final rdd in the specified output path as a text file.
		 */
		.saveAsTextFile(OUT_PATH);
		
		sc.close();
	}
}