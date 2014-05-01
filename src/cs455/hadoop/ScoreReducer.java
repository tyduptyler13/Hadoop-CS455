package cs455.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ScoreReducer extends Reducer<Text, Text, Text, DoubleWritable>{

	private MultipleOutputs<Text, DoubleWritable> out;

	public void setup(Context context){
		out = new MultipleOutputs<Text, DoubleWritable>(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		int wordCount = 0;
		int syllableCount = 0;
		int sentenceCount = 0;

		for (Text value : values){

			String svalue = value.toString();

			wordCount++;
			syllableCount += countSyllables(svalue);
			if (svalue.contains(".") || svalue.contains("!") || svalue.contains("?")){
				sentenceCount++;
			}

		}

		//Do calculations.
		double ease = 206.835 - 1.015 * ((double) wordCount / sentenceCount)
				- 84.6 * ((double) syllableCount / wordCount);
		double grade = 0.39 * ((double) wordCount / sentenceCount)
				+ 11.8 * ((double) syllableCount / wordCount) - 15.59;


		//Print calculations.

		Console.debug("Word count: " + wordCount);
		Console.debug("syllableCount: " + syllableCount);
		Console.debug("Sentence count: " + sentenceCount);
		Console.debug("Ease: " + ease);
		Console.debug("Grade: " + grade);

		out.write("Ease", key, new DoubleWritable(ease));
		out.write("Grade", key, new DoubleWritable(grade));

	}

	public void cleanup(Context c) throws IOException, InterruptedException {
		out.close();
	}

	private static final char[] vowels = { 'A', 'E', 'I', 'O', 'U', 'Y' };

	private static int countSyllables(String word){

		String currentWord = word;
		int numVowels = 0;
		boolean lastWasVowel = false;
		for (int i = 0; i < currentWord.length(); ++i){
			char wc = currentWord.charAt(i);
			boolean foundVowel = false;
			for (char v : vowels){
				//don't count diphthongs
				if (v == wc && lastWasVowel){
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
				else if (v == wc && !lastWasVowel){
					numVowels++;
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
			}

			//if full cycle and no vowel found, set lastWasVowel to false;
			if (!foundVowel)
				lastWasVowel = false;
		}
		//remove es, it's _usually? silent
		if (currentWord.length() > 2 && currentWord.substring(currentWord.length() - 2).equals("ES"))
			numVowels--;
		// remove silent e
		else if (currentWord.length() > 1 && currentWord.substring(currentWord.length() - 1).equals("E"))
			numVowels--;

		return numVowels;
	}

}
