
import java.io.IOException;
import java.util.*;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class MaxWidgetId extends Configured implements Tool {

  public static class MaxWidgetMapper
      extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Widget maxWidget = null;

    public void map(LongWritable k, Text v, Context context)
    throws IOException, InterruptedException{
      Widget widget = new Widget();
      try {
        widget.parse(v); // Auto-generated: parse all fields from text.
      } catch (ParseError pe) {
        // Got a malformed record. Ignore it.
        return;
      }

      Integer id = widget.get_id();
      String Widget_name = widget.get_widget_name();
      if (null == id || Widget_name == null) {
        return;
      } else {
        if (Widget_name != "") {
          context.write(new LongWritable(0), new Text(Widget_name));
        }
      }
    }

    // public void cleanup(Context context)
    //     throws IOException, InterruptedException {
    //   if (null != maxWidget) {
    //     context.write(new LongWritable(0), maxWidget);
    //   }
    // }
  }

  public static class MaxWidgetReducer
      extends Reducer<LongWritable, Text, Text, IntWritable> {

    // There will be a single reduce call with key '0' which gets
    // the max widget from each map task. Pick the max widget from
    // this list.
    public void reduce(LongWritable k, Iterable<Text> vals, Context context)
        throws IOException, InterruptedException {
      Widget maxWidget = null;
      HashMap<String,Integer> hm = new HashMap<String,Integer>();
      for (Text w : vals) {
        int count = 0;
          if(hm.get(w.toString())!= null) {
            count = hm.get(w.toString());
            count++;
            hm.put(w.toString(), count);
          }
          else
            hm.put(w.toString(), 1);

      }
        Set set = hm.entrySet();
        java.util.Iterator it = set.iterator();
        int max =0;
        int curr =0;
        String keyStr = "";
        while(it.hasNext())
        {
          Map.Entry pair = (Map.Entry)it.next();

          curr = (int) pair.getValue();
          if(curr > max)
          {
            keyStr = (String) pair.getKey();
            max = curr;
          }
          context.write(new Text((String) pair.getKey()), new IntWritable((int) pair.getValue()));
          //System.out.println(pair.getKey() + " - " + pair.getValue());
        }
        // context.write("=========================");
        // context.write("Mode of the Widgets name |");
        // context.write("=========================");
        context.write(new Text(keyStr), new IntWritable(max));
        // context.write("=========================");
      // if (null != maxWidget) {
      //   context.write(maxWidget, NullWritable.get());
      // }
    }
  }

  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(MaxWidgetId.class);

    job.setMapperClass(MaxWidgetMapper.class);
    job.setReducerClass(MaxWidgetReducer.class);

    FileInputFormat.addInputPath(job, new Path("widgets"));
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1; // error.
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new MaxWidgetId(), args);
    System.exit(ret);
  }
}
