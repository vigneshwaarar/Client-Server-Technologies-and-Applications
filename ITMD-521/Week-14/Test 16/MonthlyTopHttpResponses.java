import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthlyTopHttpResponses {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MonthlyTopHttpResponses <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(MonthlyTopHttpResponses.class);
    job.setJobName("MTR test 16.1.1 txt CRV");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(MonthlyTopHttpResponsesMapper.class);
    job.setReducerClass(MonthlyTopHttpResponsesReducer.class);
    job.setCombinerClass(MonthlyTopHttpResponsesReducer.class);
    job.setNumReduceTasks(4);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
