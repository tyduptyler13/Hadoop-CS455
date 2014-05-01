package cs455.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * Keep punctionation to count sentences
	 * Keep $ signs for seeing currency etc
	 * Keep - to keep compound words together
	 */
	private static final Pattern split = Pattern.compile("[^\\w'\\.!\\?\\-\\$]+");

	private static Text t = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		Text file = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

		String input = value.toString().toUpperCase(Locale.ENGLISH);
		String[] unfiltered = split.split(input);

		List<String> words = new ArrayList<String>();
		//Filter once
		for (String word : unfiltered){
			if (word.length() > 0){
				words.add(word.trim()); //Ensured to be clean now.
			}
		}

		//N-Gram(1)
		for (String word : words){
			t.set(word);
			context.write(file, t);
		}

	}

}

