import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthlyTopHttpResponsesReducer
  extends Reducer<Text, Text, Text, Text> {
  Text emitter = new Text();
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
    HashMap<String,Integer> url_detail_map = new HashMap<String,Integer>();
    int chk = 1;
    for (Text v : values) {
      String url_detail = v.toString();
      if(url_detail.length() > 1)
      {
        if(!url_detail_map.containsKey(url_detail)){
          url_detail_map.put(url_detail,chk);
        }
        else{
          url_detail_map.put(url_detail,url_detail_map.get(url_detail)+1);
          }
        }
      }
    String top_url="";
    int top_url_count = 0;
    for (String url_detail: url_detail_map.keySet())
	   {
      int tmp_url_detail = url_detail_map.get(url_detail);
      if (tmp_url_detail>top_url_count){
        top_url = url_detail;
        top_url_count = tmp_url_detail;
     }
    }
	 String strCombine = top_url + "\t" + top_url_count;
   emitter.set(strCombine);
   context.write(emitter, new Text(key));
  }
}
