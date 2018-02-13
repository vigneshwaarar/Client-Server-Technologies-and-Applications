// cc MaxTemperatureLatWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv MaxTemperatureLatWithCombiner
public class LatitudeMaxTemperatureWithCombiner {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: LatitudeMaxTemperatureWithCombiner <input path> " +
          "<output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(LatitudeMaxTemperatureWithCombiner.class);
    job.setJobName("Latitude Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(LatitudeMaxTemperatureMapper.class);
    /*[*/job.setCombinerClass(LatitudeMaxTemperatureReducer.class)/*]*/;
    job.setReducerClass(LatitudeMaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperatureLatWithCombiner
