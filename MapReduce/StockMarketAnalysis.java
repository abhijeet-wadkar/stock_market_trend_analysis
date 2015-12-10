/*
 * Project Name: TrueGen - Stock Market Analysis Prediction
 * Subject: Big Data (CMSC691)
 * Author: Team TrueGen
 * Fall-2015
 */

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StockMarketAnalysis {

  // this is the name of the jar you compile, so hadoop has its fucntion hooks
  private final static String JAR_NAME = "sma.jar";   

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    // Mapping function =
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      // Get the company name and use it as a key
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      String[] result = value.toString().split("\n");
      List<String> list = new ArrayList<String>();

      // Get all rows in a list and reverse the list
      for(String line : result) {
	list.add(line);
      }
      // arrange the days chronologically
      Collections.reverse(list);
      try {
      	for(String line : list) {
	  // as field in a of the input file are comma separated
	  // split a row according to the column
	  String[] columns = line.split(",");
	  // get the opeing price for a day
	  double opening_price = Double.parseDouble(columns[1]);
	  // get the closing price for a day
	  double closing_price = Double.parseDouble(columns[6]);
	  // calculate the percentage change for each company
	  double percentage_change = ((closing_price - opening_price) / opening_price)*100; 
	  context.write(new Text(filename), new DoubleWritable(percentage_change));
        }
     } catch(Exception e) {
	  
     }
    }
  }

  public static class IntScoreReducer
       extends Reducer<Text,DoubleWritable,Text,Text> {
    private DoubleWritable result = new DoubleWritable();

    // reduce function - calculated score for each company and %times score went up
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int score = 0;
      int total_records = 0;
      double times_increased = 0;
      int up_adjust_factor = 1;
      int down_adjust_factor = 1;
      // calculating score for each company
      for (DoubleWritable val : values) {
        // calculate total number of records
	total_records++;
        if(val.get() > 0) {
		score += up_adjust_factor + val.get();
		times_increased++;
		up_adjust_factor++;
		if(down_adjust_factor > 0)
			down_adjust_factor--;
	} else if (val.get() < 0) {
		score -= down_adjust_factor + val.get();
		down_adjust_factor++;
		if(up_adjust_factor > 0)
			up_adjust_factor--;
        }
      }
      double percentage_times_increased = (times_increased / total_records) * 100;
      result.set(score);
      // Here, Key=company symbol and Value=<score, %times_increased>
      context.write(new Text(key), new Text(new Double(score).toString()+ " " + new Double(percentage_times_increased).toString()));
    }
  }

  public static void main(String[] args) throws Exception {

    /* setup phase */
    Configuration conf = new Configuration();
    // setting the split size
    conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 10000000);
    Job job = Job.getInstance(conf, "stock market");
    job.setJar( JAR_NAME );
    job.setJarByClass(StockMarketAnalysis.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(IntScoreReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // give input data path to this MapReduce job
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // set the output path - this job's output will be put at this location
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
