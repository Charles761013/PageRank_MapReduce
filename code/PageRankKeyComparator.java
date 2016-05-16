package pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRankKeyComparator extends WritableComparator {

	public PageRankKeyComparator() {
		super(Text.class, true);
	}

	public int compare(WritableComparable o1, WritableComparable o2) {
		Text key1 = (Text) o1;
		Text key2 = (Text) o2;

		String[] s1 = key1.toString().split("-");
		String[] s2 = key2.toString().split("-");
		String offset1 = s1[2];
		String offset2 = s2[2];
		String token1 = s1[0] + s1[1];
		String token2 = s2[0] + s2[1];

		if(token1.compareTo(token2) > 0) {
			return 1;
		}
		else if(token1.compareTo(token2) < 0) {
			return -1;
		}
		else {
			if(offset1.length() > offset2.length()) {
				return 1;
			}
			else if(offset1.length() < offset2.length()) {
				return -1;
			}
			else {
				return offset1.compareTo(offset2);
			}
		}
	}
}
