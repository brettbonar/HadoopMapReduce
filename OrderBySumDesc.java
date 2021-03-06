import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// http://blog.ditullio.fr/2015/12/24/hadoop-basics-filter-aggregate-sort-mapreduce/
// https://github.com/nicomak/blog/blob/master/donors/src/main/java/mapreduce/donation/OrderBySumDesc.java
public class OrderBySumDesc {
	public static class DescendingIntComparator extends WritableComparator {

		public DescendingIntComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable key1 = (IntWritable) w1;
			IntWritable key2 = (IntWritable) w2;          
			return -1 * key1.compareTo(key2);
		}
	}

	public static class InverseCitySumMapper extends Mapper<Text, Text, IntWritable, Text> {

		private IntWritable intSum = new IntWritable();		

		@Override
		public void map(Text city, Text sum, Context context) throws IOException, InterruptedException {
			int intVal = Integer.parseInt(sum.toString());
			intSum.set(intVal);
			context.write(intSum, city);
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "Order By Sum Desc");
		job.setJarByClass(CountUserEdits.class);

		// The mapper which transforms (K:V) => (int(V):K)
		job.setMapperClass(InverseCitySumMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Sort with descending int order
		job.setSortComparatorClass(DescendingIntComparator.class);

		// Use default Reducer which simply transforms (K:V1,V2) => (K:V1), (K:V2)
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
