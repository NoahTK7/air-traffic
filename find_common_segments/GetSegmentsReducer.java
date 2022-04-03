// cc GetSegmentsReducer Reducer for maximum temperature example
// vv GetSegmentsReducer
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class GetSegmentsReducer
  extends Reducer<Text, Text, Text, IntWritable> {

//  private final Logger logger = Logger.getLogger(GetSegmentsReducer.class);
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

    HashSet<String> codes = new HashSet<>();
    for (Text code : values) {
      codes.add(code.toString());
    }

    int numUnique = codes.size();
    // param: minimum number of unique occurrences
    if (numUnique > 5) {
      context.write(key, new IntWritable(numUnique));
    }
  }
}
// ^^ GetSegmentsReducer
