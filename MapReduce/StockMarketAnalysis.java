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

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      String[] result = value.toString().split("\n");
      List<String> list = new ArrayList<String>();

      for(String line : result) {
	list.add(line);
      }
      Collections.reverse(list);
      try {
      	for(String line : list) {
	  String[] columns = line.split(",");
	  double opening_price = Double.parseDouble(columns[1]);
	  double closing_price = Double.parseDouble(columns[6]);
	  double percentage_change = ((closing_price - opening_price) / opening_price)*100; 
	  context.write(new Text(filename), new DoubleWritable(percentage_change));
        }
     } catch(Exception e) {
	  
     }
    }
  }

  public static class IntScoreReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int score = 0;
      int total_records = 0;
      double times_increased = 0;
      int up_adjust_factor = 1; // linearly increases
      int down_adjust_factor = 1; // linearly decreases
      for (DoubleWritable val : values) {
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
      context.write(new Text(key + "_score    "), result);
      result.set(percentage_times_increased);
      context.write(new Text(key + "_%times_up"), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 10000000);
    Job job = Job.getInstance(conf, "stock market");
    job.setJar( JAR_NAME );
    job.setJarByClass(StockMarketAnalysis.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntScoreReducer.class);
    //job.setNumReduceTasks(10);
    job.setReducerClass(IntScoreReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
