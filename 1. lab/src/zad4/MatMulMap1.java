import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

public class MatMulMap1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
		throws IOException {
		
		String[] parts = (value.toString()).split(" ");
		String M = parts[0];
		String r = parts[1];
		String c = parts[2];
		String v = parts[3];
		
		if (M.equals("a")) {
			output.collect(new IntWritable(Integer.parseInt(c)), new Text(M + "," + r + "," + v));
		} else {
			output.collect(new IntWritable(Integer.parseInt(r)), new Text(M + "," + c + "," + v));
		}
			
	}
}
