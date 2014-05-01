package cs455.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Engine {

	public static void main(String[] args){

		if (args.length != 2){
			Console.log("usage: [input] [output]");
			Console.log("Recieved:");
			Console.log(args);
			Console.error("Unexpected arguments. See above.");
			System.exit(-1);
		}

		Console.log("===Starting===");

		try {

			Job job = Job.getInstance(new Configuration());

			//Mapper Reducer
			job.setMapperClass(WordMapper.class);
			job.setReducerClass(ScoreReducer.class);

			//Mapper outputs
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			//Reducer outputs
			MultipleOutputs.addNamedOutput(job, "Ease", TextOutputFormat.class,
					Text.class, DoubleWritable.class);
			MultipleOutputs.addNamedOutput(job, "Grade", TextOutputFormat.class,
					Text.class, DoubleWritable.class);


			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setJarByClass(Engine.class);

			Console.log("===Running NGram===");

			job.waitForCompletion(true);

			if (job.isSuccessful()){

				Console.log("====FINISHED====");

			} else {

				Console.warn("===NGRAM Failed===");

			}

		} catch (Exception e) {

			Console.error("An error occured!");
			e.printStackTrace();
			System.exit(1);

		}


	}

}
