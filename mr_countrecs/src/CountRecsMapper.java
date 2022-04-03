import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountRecsMapper
  extends Mapper<LongWritable, Text, Text, LongWritable> {

  Text keyText = new Text("");
  private static final LongWritable one = new LongWritable(1);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    if (line.length() == 0) {
      // do not count empty lines
      return;
    }

    context.write(keyText, one);
  }
}
