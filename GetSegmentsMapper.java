// cc GetSegmentsMapper Mapper for maximum temperature example
// vv GetSegmentsMapper
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetSegmentsMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private static DecimalFormat df = new DecimalFormat("#.00");

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    try {
      String[] elements = line.split("\\|");

      float lat1 = Float.parseFloat(elements[1]);
      float lon1 = Float.parseFloat(elements[2]);

      float lat2 = Float.parseFloat(elements[3]);
      float lon2 = Float.parseFloat(elements[4]);

      if (lat1 == lat2 && lon1 == lon2) {
        // no need to even pass these occurrences to the reducer
        return;
      }

      word.set(df.format(lat1) + "|" + df.format(lon1) + "|" + df.format(lat2) + "|" + df.format(lon2));
      context.write(word, one);
    } catch (Exception e) {
      System.out.println("Exception occurred: " + e + " while processing line: " + line);
    }
  }
}
// ^^ GetSegmentsMapper
