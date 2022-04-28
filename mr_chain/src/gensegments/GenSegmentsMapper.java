package gensegments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class GenSegmentsMapper
  extends Mapper<Text, Text, FlightSnapshotKey, FlightSnapshot> {

  public final long INTERVAL = 300; // 5 minutes
  private final Logger logger = Logger.getLogger(GenSegmentsMapper.class);

  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    FlightSnapshotKey reducerKey1 = new FlightSnapshotKey();
    FlightSnapshotKey reducerKey2 = new FlightSnapshotKey();
    FlightSnapshot reducerValue = new FlightSnapshot();

    String line = value.toString();
    String[] tokens = line.split(",");

    String hex = tokens[1];
    long ts = Long.parseLong(tokens[0]);
    double lat = Double.parseDouble(tokens[2]);
    double lon = Double.parseDouble(tokens[3]);

    reducerValue.set(lat, lon);

    reducerKey1.set(hex, ts);
    context.write(reducerKey1, reducerValue);

    reducerKey2.set(hex, ts + INTERVAL);
    context.write(reducerKey2, reducerValue);
  }
}
