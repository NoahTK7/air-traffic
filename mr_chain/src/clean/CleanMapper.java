package clean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

public class CleanMapper
  extends Mapper<LongWritable, Text, Text, Text> {

  private static final Text outValue = new Text();
  private static final DecimalFormat df = new DecimalFormat("0.00");

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] tokens = line.split(",");

    String lat = tokens[2];
    String lon = tokens[3];

    tokens[2] = df.format(Double.parseDouble(lat));
    tokens[3] = df.format(Double.parseDouble(lon));

    outValue.set(String.join(",", tokens));

    context.write(null, outValue);
  }
}
