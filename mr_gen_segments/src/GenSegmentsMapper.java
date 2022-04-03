import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenSegmentsMapper
  extends Mapper<LongWritable, Text, FlightSnapshotKey, FlightSnapshot> {

  FlightSnapshotKey reducerKey = new FlightSnapshotKey();
  FlightSnapshot reducerValue = new FlightSnapshot();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] tokens = line.split(",");

    String hex = tokens[1];
    long ts = Long.parseLong(tokens[0]);
    double lat = Double.parseDouble(tokens[2]);
    double lon = Double.parseDouble(tokens[3]);
    String registration = tokens[4];
    String flightType = tokens[5];

    reducerKey.set(hex, ts);
    reducerValue.set(ts, lat, lon, registration, flightType);

    context.write(reducerKey, reducerValue);
  }
}
