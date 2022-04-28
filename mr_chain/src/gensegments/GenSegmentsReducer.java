package gensegments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class GenSegmentsReducer
  extends Reducer<FlightSnapshotKey, FlightSnapshot, Text, FlightSegment> {

  private final Logger logger = Logger.getLogger(GenSegmentsReducer.class);

  private final Text outputKey = new Text();
  private final FlightSegment outputValue = new FlightSegment();

  @Override
  public void reduce(FlightSnapshotKey key, Iterable<FlightSnapshot> values,
                     Context context)
      throws IOException, InterruptedException {

    outputKey.set(key.getHex());

//    logger.info("key = " + key);

    ArrayList<FlightSnapshot> snapshots = new ArrayList<>();
    for (FlightSnapshot snapshot : values) {
//      logger.info("snapshot = " + snapshot);
      // make deep copy of snapshot
      FlightSnapshot snapshotDeepCopy = new FlightSnapshot(snapshot);
      snapshots.add(snapshotDeepCopy);
    }
//    logger.info("sz = " + snapshots.size());

    if (snapshots.size() < 2) {
      // only one snapshot, so no segment
      // (probably within 5 minutes of end of flight/data window, so no endpoint to make a segment)
//      logger.info("only one snapshot, so no segment");
      return;
    }
    if (snapshots.size() > 2) {
      logger.warn("More than two snapshots for key " + key);
    }

//    logger.info("snapshot1 = " + snapshots.get(0));
//    logger.info("snapshot2 = " + snapshots.get(1));

    outputValue.set(snapshots.get(0), snapshots.get(1));

//    logger.info("lat1 = " + outputValue.getLat1() + ", lon1 = " + outputValue.getLon1() +
//            ", lat2 = " + outputValue.getLat2() + ", lon2 = " + outputValue.getLon2());
//    logger.info("key = " + key + ", outputValue = " + outputValue);

    context.write(outputKey, outputValue);
  }
}
