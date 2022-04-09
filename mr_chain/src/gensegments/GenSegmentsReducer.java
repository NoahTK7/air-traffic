package gensegments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class GenSegmentsReducer
  extends Reducer<FlightSnapshotKey, FlightSnapshot, Text, Text> {

  private final Text outputKey = new Text();
  private final Text outputValue = new Text();

  @Override
  public void reduce(FlightSnapshotKey key, Iterable<FlightSnapshot> values,
      Context context)
      throws IOException, InterruptedException {

    outputKey.set(key.getHex());

    Iterator<FlightSnapshot> snapshots = values.iterator();

    FlightSnapshot currentSnapshot = snapshots.next();
    while (snapshots.hasNext()) {
      // TODO: how to handle every 5 minutes?
      FlightSnapshot nextSnapshot = snapshots.next();
      FlightSegment segment = new FlightSegment(currentSnapshot, nextSnapshot);
      outputValue.set(segment.toString());
      context.write(outputKey, outputValue);
      currentSnapshot = nextSnapshot;
    }
  }
}
