package mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import mapreduce.job1.PageRankJob1InputFormat;
import mapreduce.job1.PageRankJob1Mapper;
import mapreduce.job1.PageRankJob1Partitioner;
import mapreduce.job1.PageRankJob1Reducer;
import mapreduce.job2.PageRankJob2Mapper;
import mapreduce.job2.PageRankJob2Reducer;
import mapreduce.job2.PageRankJob2GroupingComparator;
import mapreduce.job2.PageRankJob2Partitioner;
import mapreduce.job3.PageRankJob3Mapper;
import mapreduce.job3.PageRankJob3Reducer;
import mapreduce.job3.PageRankJob3aReducer;
import mapreduce.job4.PageRankJob4Mapper;
import mapreduce.job4.PageRankJob4Reducer;

public class PageRank {
	
    public static NumberFormat NF = new DecimalFormat("00");
	
	// configuration values
    public static Double DAMPING = 0.85;
    public static Double OFFSET = 0.15;
    public static int ITERATIONS = 1; //by default
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    public static String DELIMITER = "¬";

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		try {     
            // parse input parameters
            for (int i = 0; i < args.length; i += 3) {
                PageRank.IN_PATH = args[i];
                PageRank.OUT_PATH = args[i + 1];
                PageRank.ITERATIONS = Integer.parseInt(args[i + 2]);
            }
        } 
		catch(ArrayIndexOutOfBoundsException e) {
            printUsageText(e.getMessage());
            System.exit(1);
        }
		catch (NumberFormatException e) {
            printUsageText(e.getMessage());
            System.exit(1);
        }

        // check for valid parameters to be set
        if (PageRank.IN_PATH.isEmpty() || PageRank.OUT_PATH.isEmpty()) {
            printUsageText("missing required parameters");
            System.exit(1);
        }
        		
        
        // print current configuration in the console
        System.out.println("Damping factor: " + PageRank.DAMPING);
        System.out.println("Number of iterations: " + PageRank.ITERATIONS);
        System.out.println("Input directory: " + PageRank.IN_PATH);
        System.out.println("Output directory: " + PageRank.OUT_PATH);
        System.out.println("---------------------------");
        
        String inPath = null;;
        String lastOutPath = null;
        boolean isCompleted;
        PageRank pagerank = new PageRank();
        
        
        lastOutPath = OUT_PATH + "-job1";

        System.out.println("Running Job#1 (filtering out some data) ...");
        isCompleted = pagerank.job1(IN_PATH, lastOutPath);
        if (!isCompleted) {
            System.exit(1);
        }  
        
        inPath = lastOutPath;
        lastOutPath = OUT_PATH + "-job2";
        
        System.out.println("Running Job#2 (graph parsing) ...");
        isCompleted = pagerank.job2(inPath, lastOutPath);
        if (!isCompleted) {
            System.exit(1);
        }
    		
        for(int runs = 0; runs < ITERATIONS; runs++) {
            inPath = lastOutPath; 
            if(runs + 1 < ITERATIONS) {
    			lastOutPath = OUT_PATH + "-iter" + NF.format(runs + 1);
            	System.out.println("Running Job#3 [" + (runs + 1) + "-" + PageRank.ITERATIONS + "] (PageRank calculation) ...");
                isCompleted = pagerank.job3(inPath, lastOutPath);
                if (!isCompleted) {
                    System.exit(1);
                }
                inPath = lastOutPath;
		    	lastOutPath = OUT_PATH + "-update-for-iter-" + NF.format(runs + 2);
		    	System.out.println("Running Job#4 [" + (runs + 1) + "-" + PageRank.ITERATIONS + "] (updating page rank entries for the next iteration) ...");
		    	isCompleted = pagerank.job4(inPath, lastOutPath);
	            if (!isCompleted) {
	                System.exit(1);
	            }
            }
            else {
            	System.out.println("Running Job#3a [" + (runs + 1) + "-" + PageRank.ITERATIONS + "] (Final PageRank calculation) ...");
                isCompleted = pagerank.job3a(inPath, OUT_PATH);
                if (!isCompleted) {
                    System.exit(1);
                }
            }
        }
	        
        System.out.println("DONE!");
        System.exit(0);
	}
		
	/**
     * This will run the Job #1 (Filtering Input).
     * Will filter out the most recent revision of each article
     * with title as a key and the revision ID with the out-links
     * from the MAIN line.
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job1(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(PageRankJob1InputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob1Mapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
		job.setCombinerClass(PageRankJob1Reducer.class);
        job.setPartitionerClass(PageRankJob1Partitioner.class);
        
        return job.waitForCompletion(true);
     
    }
    
    /**
     * This will run the Job #2 (Graph Parsing).
     * It will parse the graph given as input the output of job1
     * in the following key-value format:
     *  <article_title> <MAIN line: out-links> 
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job2(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob2Mapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);
        job.setPartitionerClass(PageRankJob2Partitioner.class);
        job.setGroupingComparatorClass(PageRankJob2GroupingComparator.class);
		
        return job.waitForCompletion(true);
        
    }
    
    /**
     * This will run the Job #3 (Final job for the PageRank Calculation).
     *  
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job3(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        
        // output
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob3Reducer.class);
        // Might be used for more than one iteration.
		job.setPartitionerClass(PageRankJob2Partitioner.class);
		job.setGroupingComparatorClass(PageRankJob2GroupingComparator.class);

        return job.waitForCompletion(true);
        
    }
    
    /**
     * This will run the Job #3a (PageRank Calculation preparing output for next iteration).
     *  
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job3a(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #3a");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        
        // output
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob3aReducer.class);
        // Might be used for more than one iteration.
		job.setPartitionerClass(PageRankJob2Partitioner.class);
		job.setGroupingComparatorClass(PageRankJob2GroupingComparator.class);

        return job.waitForCompletion(true);
        
    }
    
    /**
     * This will run the Job #4 (Update in-links with a new page rank).
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job4(String in, String out) throws IOException, 
    												  ClassNotFoundException, 
    												  InterruptedException {

		Job job = Job.getInstance(new Configuration(), "Job #4");
		job.setJarByClass(PageRank.class);
		
		// input / mapper
		FileInputFormat.setInputPaths(job, new Path(in));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(PageRankJob4Mapper.class);
		
		// output
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(PageRankJob4Reducer.class);
		// Might be used for more than one iteration.
		job.setPartitionerClass(PageRankJob2Partitioner.class);
		job.setGroupingComparatorClass(PageRankJob2GroupingComparator.class);
		
		return job.waitForCompletion(true);
		
		}
    
    
    /**
     * Print the main an only help text in the System.out
     * 
     * @param err an optional error message to display
     */
    public static void printUsageText(String err) {
        
        if (err != null) {
            // if error has been given, print it
            System.err.println("ERROR: " + err + ".\n");
        }
    }
}
