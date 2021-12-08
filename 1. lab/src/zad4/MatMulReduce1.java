import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatMulReduce1 extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		List<String> list_a = new ArrayList<String>();
		List<String> list_b = new ArrayList<String>();
		
		while (values.hasNext()) {
			String current = values.next().toString();
			String[] parts = current.split(",");
			//result *= Integer.parseInt(parts[2]);
			if (parts[0].equals("a")) {
				list_a.add(current);
			} else {
				list_b.add(current);
			}
        }
		for (String a_member : list_a) {
			String[] a_parts = a_member.split(",");
			for (String b_member : list_b) {
				String[] b_parts = b_member.split(",");
				String new_key = a_parts[1] + " " + b_parts[1];
				int result = Integer.parseInt(a_parts[2]) * Integer.parseInt(b_parts[2]);
				output.collect(new Text(new_key), new IntWritable(result));	
			}
		}
				
	}
}
