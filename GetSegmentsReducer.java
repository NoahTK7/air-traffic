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

  private Logger logger = Logger.getLogger(GetSegmentsReducer.class);
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

//    logger.info("key: " + key.toString());

    HashSet<String> codes = new HashSet<>();
    for (Text code : values) {
      codes.add(code.toString());
//      logger.info("[" + key.toString() + "] val: " + code.toString());
    }

    int numUnique = codes.size();
//    logger.info("set: " + codes);
//    logger.info("len: " + codes.size());

    // param: minimum number of unique occurrences
    if (numUnique > 5) {
      context.write(key, new IntWritable(numUnique));
    }
  }
}
// ^^ GetSegmentsReducer
