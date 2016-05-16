package pagerank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import pagerank.PageRank.PageCount;
import java.lang.StringBuilder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

public class BuildGraphMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text title = new Text();
	private Text link = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String valueString = value.toString();
		StringBuffer linkBuffer = new StringBuffer();
		Pattern patternTitle = Pattern.compile("<title>(.+?)</title>");
		Pattern patternLink = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");

		Matcher matcherTitle = patternTitle.matcher(valueString);
		if(matcherTitle.find())
		{
			String titleDecoded = unescape(matcherTitle.group(1));
			title.set(titleDecoded); //set key
			context.getCounter(PageCount.Count).increment(1);
			//make a title set
			context.write(new Text("    "), new Text(titleDecoded));
		}
		else
		{
			//can't find title
			return;
		}

		Matcher matcherLink = patternLink.matcher(valueString);
		while(matcherLink.find()){
			String linkDecoded = unescape(matcherLink.group(1));
			int decodeLength = linkDecoded.length();
			if(linkDecoded == null || linkDecoded.equals("") || (decodeLength < 1)) {
				continue;
			}
			linkBuffer.setLength(0); //clear buffer
			linkBuffer.append(linkDecoded.substring(0, 1).toUpperCase());
			if(decodeLength > 1) {
				linkBuffer.append(linkDecoded.substring(1));
			}
			link.set(linkBuffer.toString());
			context.write(title, link);

		}
		if(linkBuffer.toString().equals(""))
		{
			//means no out links
			context.write(title, link);
		}

	}

	public static String unescape(String text) {
		StringBuilder result = new StringBuilder(text.length());
		int i = 0;
		int n = text.length();
		while (i < n) {
			char charAt = text.charAt(i);
			if (charAt != '&') {
				result.append(charAt);
				i++;
			} else {
				if (text.startsWith("&amp;", i)) {
					result.append('&');
					i += 5;
				} else if (text.startsWith("&apos;", i)) {
					result.append('\'');
					i += 6;
				} else if (text.startsWith("&quot;", i)) {
					result.append('"');
					i += 6;
				} else if (text.startsWith("&lt;", i)) {
					result.append('<');
					i += 4;
				} else if (text.startsWith("&gt;", i)) {
					result.append('>');
					i += 4;
				} else i++;
			}
		}
		return result.toString();
	}
}
