import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class VideoSort {
	public static void main (String[] args) throws Exception {
		VideoCount.countViews(args);

		JobConf conf2 = new JobConf(VideoSort.class);
		conf2.setJobName("Video sort");
		FileInputFormat.addInputPath(conf2, new Path("temp_out/part-00000"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
		
		conf2.setMapperClass(VideoSortMap.class);
		conf2.setReducerClass(VideoSortReduce.class);
		
		conf2.setMapOutputKeyClass(IntWritable.class);
		conf2.setMapOutputValueClass(Text.class);
		
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);
		
		conf2.setOutputKeyComparatorClass(MyComparator.class);
		JobClient.runJob(conf2);
	}
	
}
