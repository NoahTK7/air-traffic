package gensegments;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenSegmentsMapper
  extends Mapper<Text, Text, FlightSnapshotKey, FlightSnapshot> {

  public final long INTERVAL = 300; // 5 minutes

  FlightSnapshotKey reducerKey = new FlightSnapshotKey();
  FlightSnapshot reducerValue = new FlightSnapshot();

  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] tokens = line.split(",");

    String hex = tokens[1];
    long ts = Long.parseLong(tokens[0]);
    double lat = Double.parseDouble(tokens[2]);
    double lon = Double.parseDouble(tokens[3]);

    reducerKey.set(hex, ts);
    reducerValue.set(lat, lon);

    context.write(reducerKey, reducerValue);

    reducerKey.set(hex, ts + INTERVAL);

    context.write(reducerKey, reducerValue);
  }
}
