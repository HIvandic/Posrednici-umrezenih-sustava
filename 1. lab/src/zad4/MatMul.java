import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MatMul {
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: MatMul <input path> <output path>");
			System.exit(-1);
		}
		JobConf conf = new JobConf(MatMul.class);
		conf.setJobName("Matrix multiplication");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("temp_out/"));
		conf.setMapperClass(MatMulMap1.class);
		conf.setReducerClass(MatMulReduce1.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);
		
		JobConf conf2 = new JobConf(MatMul.class);
		conf2.setJobName("Video sort");
		FileInputFormat.addInputPath(conf2, new Path("temp_out/part-00000"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
		
		conf2.setMapperClass(MatMulMap2.class);
		conf2.setReducerClass(MatMulReduce2.class);
		
		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(IntWritable.class);
		
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf2);
	}
}
