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

class StockKey implements WritableComparable<StockKey> {

	private String symbol;
	private Double percentageChange;

	public StockKey() {
	}

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

	public int compareTo(StockKey o) {
		int result = symbol.compareTo(o.symbol);
		if (0 == result) {
			result = percentageChange.compareTo(o.percentageChange);
		}
		return result;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Double getpercentageChange() {
		return percentageChange;
	}

	public void setpercentageChange(Double percentageChange) {
		this.percentageChange = percentageChange;
	}
}

class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(StockKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		StockKey k1 = (StockKey) w1;
		StockKey k2 = (StockKey) w2;

		int result = k1.getSymbol().compareTo(k2.getSymbol());
		if (0 == result) {
			result = k1.getpercentageChange().compareTo(k2.getpercentageChange());
		}
		return result;
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

		return k1.getSymbol().compareTo(k2.getSymbol());
	}
}

class NaturalKeyPartitioner extends Partitioner<StockKey, DoubleWritable> {

	@Override
	public int getPartition(StockKey key, DoubleWritable val, int numPartitions) {
		int hash = key.getSymbol().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}

public class StockMarketAnalysisGraph {

	private final static String JAR_NAME = "sma.jar";
	
	private static Log log = LogFactory.getLog(StockMarketAnalysisGraph.class); 

	public static class TokenizerMapper extends
			Mapper<Object, Text, StockKey, DoubleWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName().toString().split("\\.")[0];
			String[] result = value.toString().split("\n");

			for (String line : result) {
				try {
					String[] columns = line.split(",");
					double opening_price = Double.parseDouble(columns[1]);
					double closing_price = Double.parseDouble(columns[6]);
					double percentage_change = ((closing_price - opening_price) / opening_price) * 100;
					context.write(new StockKey(filename, percentage_change),
							new DoubleWritable(percentage_change));
				} catch (Exception e) {
					System.out.println("Skpping first line");
				}
			}
		}
	}

	public static class IntScoreReducer extends
			Reducer<StockKey, DoubleWritable, DoubleWritable, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		MultipleOutputs<DoubleWritable, DoubleWritable> mos;

		
		public void setup(Context context) {
			mos = new MultipleOutputs<DoubleWritable, DoubleWritable>(context);
		}

		public void reduce(StockKey key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			int count = 0;
			List<Double> list = new ArrayList<Double>();
			for (DoubleWritable value : values) {
				list.add(new Double(value.get()));
				count++;
			}

			double index = 1;
			for (Double value : list) {
				log.info(key.getSymbol() + " " +value.doubleValue());
				mos.write(key.getSymbol(), new DoubleWritable(index/count), new DoubleWritable(value.doubleValue()));
				index++;
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
	        mos.close();
	    }
	}

	public static void main(String[] args) throws Exception {
				
		Configuration conf = new Configuration();
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 1000000);
		Job job = Job.getInstance(conf, "stock market");
		job.setJar(JAR_NAME);
		job.setJarByClass(StockMarketAnalysisGraph.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setReducerClass(IntScoreReducer.class);
		job.setMapOutputKeyClass(StockKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
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