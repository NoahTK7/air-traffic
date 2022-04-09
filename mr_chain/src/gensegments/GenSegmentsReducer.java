package gensegments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class GenSegmentsReducer
  extends Reducer<FlightSnapshotKey, FlightSnapshot, Text, FlightSegment> {

  private final Logger LOG = Logger.getLogger(GenSegmentsReducer.class);

  private final Text outputKey = new Text();
  private final FlightSegment outputValue = new FlightSegment();

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
    outputValue.set(snapshot1, snapshot2);
    context.write(outputKey, outputValue);
  }
}
