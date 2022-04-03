import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  Text keyText = new Text("Total number of lines in AirBNB file:");
  private static final IntWritable one = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    context.write(keyText, one);
  }
}
