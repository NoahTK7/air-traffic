package gensegments;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightSegment implements Writable {
    private final DoubleWritable lat1 = new DoubleWritable();
    private final DoubleWritable lon1 = new DoubleWritable();
    private final DoubleWritable lat2 = new DoubleWritable();
    private final DoubleWritable lon2 = new DoubleWritable();

    public FlightSegment() {
    }

    public FlightSegment(FlightSnapshot fs1, FlightSnapshot fs2) {
        this.lat1.set(fs1.getLat().get());
        this.lon1.set(fs1.getLon().get());
        this.lat2.set(fs2.getLat().get());
        this.lon2.set(fs2.getLon().get());
    }

    public FlightSegment(double lat1, double lon1, double lat2, double lon2) {
        this.lat1.set(lat1);
        this.lon1.set(lon1);
        this.lat2.set(lat2);
        this.lon2.set(lon2);
    }

    public void set(double lat1, double lon1, double lat2, double lon2) {
        this.lat1.set(lat1);
        this.lon1.set(lon1);
        this.lat2.set(lat2);
        this.lon2.set(lon2);
    }

    public void set(FlightSnapshot fs1, FlightSnapshot fs2) {
        this.lat1.set(fs1.getLat().get());
        this.lon1.set(fs1.getLon().get());
        this.lat2.set(fs2.getLat().get());
        this.lon2.set(fs2.getLon().get());
    }

    public DoubleWritable getLat1() {
        return lat1;
    }

    public DoubleWritable getLon1() {
        return lon1;
    }

    public DoubleWritable getLat2() {
        return lat2;
    }

    public DoubleWritable getLon2() {
        return lon2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        lat1.write(dataOutput);
        lon1.write(dataOutput);
        lat2.write(dataOutput);
        lon2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        lat1.readFields(dataInput);
        lon1.readFields(dataInput);
        lat2.readFields(dataInput);
        lon2.readFields(dataInput);
    }

    public String toString() {
        return lat1 + "," + lon1 + "|" + lat2 + "," + lon2;
    }
}
