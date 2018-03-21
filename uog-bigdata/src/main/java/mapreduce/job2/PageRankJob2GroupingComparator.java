package mapreduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import mapreduce.PageRank;

public class PageRankJob2GroupingComparator extends WritableComparator {
    protected PageRankJob2GroupingComparator() {
        super(Text.class, true);
      }
    
    @Override
     /**
      * This comparator groups together all the composite key pairs
      * with the same natural key as a result to be processed by a single reducer.
      */
     public int compare(WritableComparable wc1, WritableComparable wc2) {
         Text compKey1 = (Text) wc1;
         Text compKey2 = (Text) wc2;
         String[] compKey1Items = compKey1.toString().split(PageRank.DELIMITER);
         String[] compKey2Items = compKey2.toString().split(PageRank.DELIMITER);
         String natKey1 = compKey1Items[0];
         String natKey2 = compKey2Items[0];
         
         return natKey1.compareTo(natKey2);
     }
}
