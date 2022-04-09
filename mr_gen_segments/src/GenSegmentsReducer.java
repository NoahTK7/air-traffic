import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class GenSegmentsReducer
  extends Reducer<FlightSnapshotKey, FlightSnapshot, Text, Text> {

  private final Logger LOG = Logger.getLogger(GenSegmentsReducer.class);

  private final Text outputKey = new Text();
  private final Text outputValue = new Text();

  @Override
  public void reduce(FlightSnapshotKey key, Iterable<FlightSnapshot> values,
      Context context)
      throws IOException, InterruptedException {

    outputKey.set(key.getHex());

    Iterator<FlightSnapshot> snapshots = values.iterator();

    FlightSnapshot snapshot1 = snapshots.next();
    if (!snapshots.hasNext()) {
      // only one snapshot, so no segment
      // (probably within 5 minutes of end of flight/data window, so no endpoint to make a segment)
      return;
    }
    FlightSnapshot snapshot2 = snapshots.next();
    if (snapshots.hasNext()) {
      LOG.warn("More than two snapshots for key " + key.getHex());
    }
    FlightSegment segment = new FlightSegment(snapshot1, snapshot2);
    outputValue.set(segment.toString());
    context.write(outputKey, outputValue);
  }
}
