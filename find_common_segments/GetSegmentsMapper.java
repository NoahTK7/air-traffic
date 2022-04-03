// cc GetSegmentsMapper Mapper for maximum temperature example
// vv GetSegmentsMapper
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class GetSegmentsMapper
  extends Mapper<LongWritable, Text, Text, Text> {

  private final Text keyText = new Text();
  private final Text valText = new Text();
  // param: number of decimal places in coordinates to round
  private static final DecimalFormat df = new DecimalFormat("#.00");
//  private final Logger logger = Logger.getLogger(GetSegmentsMapper.class);

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

      keyText.set(df.format(lat1) + "|" + df.format(lon1) + "|" + df.format(lat2) + "|" + df.format(lon2));
      valText.set(elements[0]);
      context.write(keyText, valText);
    } catch (Exception e) {
      System.out.println("Exception occurred: " + e + " while processing line: " + line);
    }
  }
}
// ^^ GetSegmentsMapper
