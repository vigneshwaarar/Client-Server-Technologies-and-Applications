// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighVisibilityMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 999999;

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String year = line.substring(15, 19);
    int highVisibility;
      highVisibility = Integer.parseInt(line.substring(78, 84));

    String quality = line.substring(84, 85);
    if (highVisibility != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(highVisibility));
    }
  }
}
// ^^ MaxTemperatureMapper
