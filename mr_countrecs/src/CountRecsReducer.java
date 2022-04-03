import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountRecsReducer
  extends Reducer<Text, IntWritable, Text, LongWritable> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

    long sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    
    context.write(new Text("Number of records: "), new LongWritable(sum));
  }
}
