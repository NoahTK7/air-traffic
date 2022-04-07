import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightSnapshotKey
        implements Writable, WritableComparable<FlightSnapshotKey> {

    private final Text hex = new Text();
    private final LongWritable timestamp = new LongWritable();

    public FlightSnapshotKey() {
    }

    public FlightSnapshotKey(String hex, long timestamp) {
        this.hex.set(hex);
        this.timestamp.set(timestamp);
    }

    public void set(String hex, long timestamp) {
        this.hex.set(hex);
        this.timestamp.set(timestamp);
    }

    public Text getHex() {
        return hex;
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        hex.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hex.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public int compareTo(FlightSnapshotKey other) {
        int compareValue = this.hex.compareTo(other.getHex());
        if (compareValue == 0) {
            compareValue = timestamp.compareTo(other.getTimestamp());
        }
        return compareValue;    // sort ascending
        //return -1*compareValue;   // sort descending
    }

    @Override
    public String toString() {
        return hex + "," + timestamp;
    }
}
