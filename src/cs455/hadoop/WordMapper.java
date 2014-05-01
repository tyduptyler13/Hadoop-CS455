package cs455.hadoop;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * Keep punctionation to count sentences
	 * Keep $ signs for seeing currency etc
	 * Keep - to keep compound words together
	 */
	private static final Pattern split = Pattern.compile("[^\\w'\\.!\\?]+");

	private static Text t = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		Text file = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

		String input = value.toString().toUpperCase(Locale.ENGLISH);
		String[] unfiltered = split.split(input);

		//All words.
		for (String word : unfiltered){
			String tmp = word.trim();
			if (tmp.length() > 0){ //Filter weird extras that still get in.
				t.set(tmp);
				context.write(file, t);
			}
		}

	}

}

