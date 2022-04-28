package clean;

import gensegments.FlightSegment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class RemoveStationaryMapper
        extends Mapper<Text, FlightSegment, Text, Text> {

    private final Text outValue = new Text();

    @Override
    public void map(Text key, FlightSegment value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] coords = line.split(",");

        double lat1 = Double.parseDouble(coords[0]);
        double lon1 = Double.parseDouble(coords[1]);
        double lat2 = Double.parseDouble(coords[2]);
        double lon2 = Double.parseDouble(coords[3]);

        // calculate distance between two points (distance formula)
        double distance = Math.sqrt(Math.pow(lat1 - lat2, 2) + Math.pow(lon1 - lon2, 2));

        if (lat1 == lat2 && lon1 == lon2) {
            // start point is the same as end point, so don't write to output
            return;
        }

        outValue.set(line + "\t" + distance);

        context.write(key, outValue);
    }
}
