// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighVisibility {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: HighVisibility <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(HighVisibility.class);
    job.setJobName("High Visibility");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(HighVisibilityMapper.class);
    job.setReducerClass(HighVisibilityReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
