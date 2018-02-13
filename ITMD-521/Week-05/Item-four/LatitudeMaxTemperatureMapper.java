// cc MaxTemperatureLatMapper Mapper for maximum temperature example
// vv MaxTemperatureLatMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LatitudeMaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String strYear = "";
    String Range = "";
    if (line.charAt(28) == '+') { // parseInt doesn't like leading plus signs
         if (line.charAt(29) == '0'){
               strYear = line.substring(29, 31);
               Range = "latitude 0-10";
               }
         if (line.charAt(29) == '1'){
             strYear = line.substring(29, 31);
             Range = "latitude 10-20";
             }

         if (line.charAt(29) == '2'){
             strYear = line.substring(29, 31);
             Range = "latitude 20-30";
             }

         if (line.charAt(29) == '3'){
             strYear = line.substring(29, 31);
             Range = "latitude 30-40";
             }

         if (line.charAt(29) == '4'){
             strYear = line.substring(29, 31);
             Range = "latitude 40-50";
             }
         if (line.charAt(29) == '5'){
             strYear = line.substring(29, 31);
             Range = "latitude 50-60";
             }
         if (line.charAt(29) == '6'){
             strYear = line.substring(29, 31);
             Range = "latitude 60-70";
             }

         if (line.charAt(29) == '7'){
             strYear = line.substring(29, 31);
             Range = "latitude 70-80";
             }
         if (line.charAt(29) == '8'){
             strYear = line.substring(29, 31);
             Range = "latitude 80-90";
             }
         if (line.charAt(29) == '9'){
             strYear = line.substring(29, 31);
             Range = "latitude 90";
             }
          }
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]" )&& Range != "") {
      context.write(new Text(Range), new IntWritable(airTemperature));
    }
  }
}
// ^^ MaxTemperatureLatMapper
