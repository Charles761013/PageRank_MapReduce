package pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PRSortingKeyComparator extends WritableComparator {

	public PRSortingKeyComparator() {
		super(Text.class, true);
	}

	public int compare(WritableComparable o1, WritableComparable o2) {
		Text key1 = (Text) o1;
		Text key2 = (Text) o2;

		String[] s1 = key1.toString().split("#");
		String[] s2 = key2.toString().split("#");
		String score1 = s1[0];
		String score2 = s2[0];
		String title1 = s1[1];
		String title2 = s2[1];
		double pr1 = Double.parseDouble(score1);
		double pr2 = Double.parseDouble(score2);

		if(pr1 > pr2) {
			return -1;
		}
		else if (pr1 < pr2) {
			return 1;
		}
		else { //equal
			if (title1.compareTo(title2) > 0) {
				return 1;
			}
			else if (title1.compareTo(title2) < 0) {
				return -1;
			}
			else {
				return 0;
			}
		}
	}
}
