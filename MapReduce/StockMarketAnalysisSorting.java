/*
 * Project Name: TrueGen - Stock Market Analysis & Prediction
 * Subject: Big Data (CMSC691)
 * Author: Team TrueGen
 * Fall-2015
 */
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// StockKey class for manupilating a composite key
class StockKey implements WritableComparable<StockKey> {

	private String symbol;
	private Double percentageChange;

	// zero argument constructor
	public StockKey() {
	}

	// constructor with arguments
	public StockKey(String symbol, Double percentageChange) {
		this.symbol = symbol;
		this.percentageChange = percentageChange;
	}

	public String toString() {
		return (new StringBuilder()).append('{').append(symbol).append(',')
				.append(percentageChange).append('}').toString();
	}

	public void readFields(DataInput in) throws IOException {
		symbol = WritableUtils.readString(in);
		percentageChange = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, symbol);
		out.writeDouble(percentageChange);
	}

	// compare key according to both symbol and percentage change
	public int compareTo(StockKey o) {
		// sorting accoring to second part of stockkey rather than symbol
		return (o.percentageChange.compareTo(percentageChange));
	}

	// get symbol from stockkey
	public String getSymbol() {
		return symbol;
	}

	// set symbol
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	
	// get percentage change from stock key
	public Double getPercentageChange() {
		return percentageChange;
	}

	// set percentage change
	public void setpercentageChange(Double percentageChange) {
		this.percentageChange = percentageChange;
	}
}


class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(StockKey.class, true);
	}

	// compare two stock keys
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// first typecast to appropriate object
		StockKey k1 = (StockKey) w1;
		StockKey k2 = (StockKey) w2;

		// compare percentage change rather than symbol
		return (k2.getPercentageChange().compareTo(k1.getPercentageChange()));
	}
}

class NaturalKeyGroupingComparator extends WritableComparator {

	protected NaturalKeyGroupingComparator() {
		super(StockKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		StockKey k1 = (StockKey) w1;
		StockKey k2 = (StockKey) w2;

		return k2.getSymbol().compareTo(k1.getSymbol());
	}
}

// used hashing mechanism
class NaturalKeyPartitioner extends Partitioner<StockKey, DoubleWritable> {

	@Override
	public int getPartition(StockKey key, DoubleWritable val, int numPartitions) {
		int hash = key.getSymbol().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}

public class StockMarketAnalysisSorting {

	private final static String JAR_NAME = "sma.jar";
	
	private static Log log = LogFactory.getLog(StockMarketAnalysisSorting.class); 

	public static class TokenizerMapper extends
			Mapper<Object, Text, StockKey, DoubleWritable> {

		// Mapping function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] result = value.toString().split("\n");
			for (String line : result) {
				try {
					String[] columns = line.split("\\s+");

					// floor all the values for convenience
					double score = Double.parseDouble(columns[1]);
					double percentage_times_up = Double.parseDouble(columns[2]);
					context.write(new StockKey(columns[0], score),
							new DoubleWritable(percentage_times_up));

				} catch (Exception e) {
					System.out.println("Exception in the mapper function" + e);
				}
			}
		}
	}

	public static class SortingReducer extends
			Reducer<StockKey, DoubleWritable, Text, Text> {
		// Reduce functon
		public void reduce(StockKey key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			// for each value of a key
			for (DoubleWritable value : values) {
				// restructure the key and value to the orginal format (same as before mapping function)
				context.write(new Text(key.getSymbol()), new Text(key.getPercentageChange().toString() + " " + Double.toString(value.get())));
			}
		}
	}

	public static void main(String[] args) throws Exception {
				
		Configuration conf = new Configuration();
		// set splitsize to an appropriate value
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 1000000);
		Job job = Job.getInstance(conf, "stock market");
		job.setJar(JAR_NAME);
		job.setJarByClass(StockMarketAnalysisSorting.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setReducerClass(SortingReducer.class);
		job.setMapOutputKeyClass(StockKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 1. Get the Configuration instance
		Configuration configuration = new Configuration();
		// 2. Get the instance of the HDFS
		FileSystem hdfs = FileSystem.get(configuration);
		// 3. Get the metadata of the desired directory
		FileStatus[] fileStatus = hdfs.listStatus(new Path(args[0]));
		// 4. Using FileUtil, getting the Paths for all the FileStatus
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		// 5. Iterate through the directory and display the files in it
		for (Path path : paths) {
			FileInputFormat.addInputPath(job, path);
			MultipleOutputs.addNamedOutput(job, path.getName().toString().split("\\.")[0],
					TextOutputFormat.class, DoubleWritable.class,
					DoubleWritable.class);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
