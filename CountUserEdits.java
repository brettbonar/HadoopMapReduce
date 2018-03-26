import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;

public class CountUserEdits {
  // https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
  // http://blog.ditullio.fr/2015/12/24/hadoop-basics-filter-aggregate-sort-mapreduce/
  public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey k1 = (CompositeKey)w1;
        CompositeKey k2 = (CompositeKey)w2;
         
        // int result = k1.getSymbol().compareTo(k2.getSymbol());
        // if(0 == result) {
        //     result = -1* k1.getCount().compareTo(k2.getCount());
        // }
        int reuslt = k1.getCount().compareTo(k2.getCount());
        return result;
    }
  }
  public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey k1 = (CompositeKey)w1;
        CompositeKey k2 = (CompositeKey)w2;
         
        return k1.getSymbol().compareTo(k2.getSymbol());
    }
  }
  public class NaturalKeyPartitioner extends Partitioner<CompositeKey, IntWritable> {
 
    @Override
    public int getPartition(CompositeKey key, IntWritable val, int numPartitions) {
        int hash = key.getSymbol().hashCode();
        int partition = hash % numPartitions;
        return partition;
    } 
  }

  
// public class SsMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {

// 	private static final Log _log = LogFactory.getLog(SsMapper.class);
	
// 	@Override
// 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
// 		String[] tokens = value.toString().split(",");
		
// 		String symbol = tokens[0].trim();
// 		Long timestamp = Long.parseLong(tokens[1].trim());
// 		Double v = Double.parseDouble(tokens[2].trim());
		
// 		CompositeKey CompositeKey = new CompositeKey(symbol, timestamp);
// 		IntWritable stockValue = new IntWritable(v);
		
// 		context.write(CompositeKey, stockValue);
// 		_log.debug(CompositeKey.toString() + " => " + stockValue.toString());
// 	}
// }


  public static class TokenizerMapper
       extends Mapper<Object, Text, CompositeKey, LongWritable>{

    private final static LongWritable one = new LongWritable(1);
    private Text userId = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String[] tokens = value.toString().split("\\s+");

      userId.set(tokens[0].trim());

      CompositeKey compositeKey = new CompositeKey(userId, one);

      context.write(compositeKey, one);
    }
  }
  
// public class SsReducer extends Reducer<StockKey, IntWritable, Text, Text> {

// 	private static final Log _log = LogFactory.getLog(SsReducer.class);
	
// 	@Override
// 	public void reduce(StockKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
// 		Text k = new Text(key.toString());
// 		int count = 0;
		
// 		Iterator<IntWritable> it = values.iterator();
// 		while(it.hasNext()) {
// 			Text v = new Text(it.next().toString());
// 			context.write(k, v);
// 			_log.debug(k.toString() + " => " + v.toString());
// 			count++;
// 		}
		
// 		_log.debug("count = " + count);
// 	}
// }

  public static class IntSumReducer
       extends Reducer<CompositeKey,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(CompositeKey key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key.getSymbol(), result);
    }
  }

  
// public class SsJob extends Configured implements Tool {

// 	/**
// 	 * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
// 	 * @param args
// 	 * @throws Exception
// 	 */
// 	public static void main(String[] args) throws Exception {
// 		ToolRunner.run(new Configuration(), new SsJob(), args);
// 	}
	
// 	@Override
// 	public int run(String[] args) throws Exception {
// 		Configuration conf = getConf();
// 		Job job = new Job(conf, "secondary sort");
		
// 		job.setJarByClass(SsJob.class);
// 		job.setPartitionerClass(NaturalKeyPartitioner.class);
// 		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
// 		job.setSortComparatorClass(CompositeKeyComparator.class);
		
// 		job.setMapOutputKeyClass(CompositeKey.class);
// 		job.setMapOutputValueClass(IntWritable.class);
		
// 		job.setOutputKeyClass(Text.class);
// 		job.setOutputValueClass(Text.class);
		
// 		job.setInputFormatClass(TextInputFormat.class);
// 		job.setOutputFormatClass(TextOutputFormat.class);
		
// 		job.setMapperClass(SsMapper.class);
// 		job.setReducerClass(SsReducer.class);
		
// 		job.waitForCompletion(true);
		
// 		return 0;
// 	}

// }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(CountUserEdits.class);

		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
    job.setSortComparatorClass(CompositeKeyComparator.class);
    
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(IntWritable.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
