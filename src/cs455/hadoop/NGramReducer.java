package cs455.hadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramReducer extends Reducer<Text, IntWritable, Text, Text> {

	//Sorted on year.
	private final TreeMap<IntWritable, Integer> wordsByDecade = new TreeMap<IntWritable, Integer>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		//Read in the years that word shows up.
		//Increment a counter on that year.
		for (IntWritable value : values){
			increment(value);
		}

		for (IntWritable decade : wordsByDecade.keySet()){
			//NGram|year|count
			context.write(key, new Text(decade.toString() + "\t" + wordsByDecade.get(decade).toString()));
		}

	}

	/**
	 * Increments the words use in a decade.
	 * @param year
	 */
	private void increment(IntWritable year){

		Integer value = wordsByDecade.get(year);

		if (value == null){
			value = new Integer(0);
		}

		wordsByDecade.put(year, new Integer(value.intValue() + 1));

	}

}
