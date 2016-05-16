package pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRankGroupComparator extends WritableComparator {

    protected PageRankGroupComparator() {
        super(Text.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        Text t1 = (Text) w1;
        Text t2 = (Text) w2;

        String[] s1 = t1.toString().split("-");
        String[] s2 = t2.toString().split("-");
        String token1 = s1[0];
        String token2 = s2[0];

        return token1.compareTo(token2);
    }
}
