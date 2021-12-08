import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class VideoSortReduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		while (values.hasNext()) {
            output.collect(values.next(), key);
        }
		
	}
}
