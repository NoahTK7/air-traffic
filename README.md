# Aviation Highways

## Summary
Utilize available flight location data to find the most heavily traveled segments of the skies that corrospond to defined routes that planes generally use.

## Data
ADS-B Exchange provides snapshots of global flight data every 5 seconds. They provide this data for free download from the first of every month. Available here: https://www.adsbexchange.com/data/#sample

## Preprocessing

### Step 1 -- Download
I downloaded the ASD-B snapshot data from the 1st of the past 12 months (available as gzip archives), and extracted the raw json files. (See get_files.py script ((link needed)).)

### Step 2 -- Flight Segment Transformation
I transformed the snapshot data into spacial flight segments. Look at 2 consecutive snapshots, and find all planes that are in both snapshots. The flight segment for that plane is the location data (coordinates) from both snapshots: `Loaction1<latitude, longitude>, Location2<latitude, longitude>`. When considering the segements between snapshots, 5 seconds is too little time for planes to move a useful distance to be tracked. So, I chose to iterate through the snapshots in steps of 5 minutes to allow for significant flight segments to form. (I also iterate through the snapshots with every offset between 0 and 300 seconds to ensure I do not miss flight segments because they didn't happen during a time divisable by 5 minutes. For example, you can count by 5 starting at 0,1,2,3,4 as to hit every number between 0 and 100.)

For latitude and longitude, 2 degress of precision means the location is accurate to within 1.11 kilometers (according to http://wiki.gis.com/wiki/index.php/Decimal_degrees). This was a jusgment call in conjunction with selecting 5 minute intervals for flight segments. The precision of coordinates and length of interval could be an area for optimization.


### Step 3 -- MapReduce

#### Mapper
