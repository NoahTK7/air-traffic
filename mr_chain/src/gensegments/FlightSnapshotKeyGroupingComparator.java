package gensegments;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightSnapshotKeyGroupingComparator
        extends WritableComparator {
    public FlightSnapshotKeyGroupingComparator() {
        super(FlightSnapshotKey.class, true);
    }

        /**
         * This comparator controls which keys are grouped together into a
         * single call to the reduce method (all keys with same hex, but any timestamp).
         * This class basically defines how keys are grouped based on which keys are considered equivalent.
        */
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            FlightSnapshotKey pair = (FlightSnapshotKey) wc1;
            FlightSnapshotKey pair2 = (FlightSnapshotKey) wc2;
            return pair.getHex().compareTo(pair2.getHex());
        }
}
