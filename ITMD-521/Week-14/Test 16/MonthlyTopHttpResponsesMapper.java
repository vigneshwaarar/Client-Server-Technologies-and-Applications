import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonthlyTopHttpResponsesMapper
  extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String yearMonth = line.substring(0, 7);
    int start = line.indexOf("/");
    String end = line.substring(start);
    int urlEnd = end.indexOf(" - ");
    String url = end.substring(1,(urlEnd));
    if(yearMonth.charAt(0) != '#' && line.charAt(1) == '0' && line.charAt(2) == '1' && (line.charAt(3) == '2' || line.charAt(3) == '3') && !line.contains("/index.") && line.contains(" 200 "))
    {
      context.write(new Text(yearMonth), new Text(url));
    }
  }
}
