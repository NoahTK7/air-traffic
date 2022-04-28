if __name__ == '__main__':
    # get 2 arguments from command line
    # 1st argument is the path to the csv file
    # 2nd argument is the path to the geojson file
    import sys
    import csv
    import json
    import os

    if len(sys.argv) != 3:
        print('Usage: gen_geojson.py <csv_file> <geojson_file>')
        sys.exit(1)
    
    csv_file = sys.argv[1]
    geojson_file = sys.argv[2]

    # check if the csv file exists
    if not os.path.isfile(csv_file):
        print('Error: csv file does not exist')
        sys.exit(1)
    
    # check if the geojson file exists
    if os.path.isfile(geojson_file):
        print('Error: geojson file already exists')
        sys.exit(1)

    # open the csv file
    with open(csv_file, 'r') as csv_f:
        # read the csv file
        csv_reader = csv.reader(csv_f)
        # skip the header
        # next(csv_reader)
        # create a list of dictionaries
        data = []
        for row in csv_reader:
            # create a dictionary
            d = {}
            # add the properties
            d['lat1'] = row[0]
            d['lon1'] = row[1]
            d['lat2'] = row[2]
            d['lon2'] = row[3]
            d['frequency'] = row[4]
            # add the dictionary to the list
            data.append(d)
        
        # create a geojson file
        with open(geojson_file, 'w') as geojson_f:
            # create a geojson object
            geojson = {
                'type': 'FeatureCollection',
                'features': []
            }
            # loop through the data
            for d in data:
                # create a geojson feature
                feature = {
                    'type': 'Feature',
                    'properties': {
                        'frequency': d['frequency']
                    },
                    'geometry': {
                        'type': 'LineString',
                        'coordinates': [[float(d['lon1']), float(d['lat1'])], [float(d['lon2']), float(d['lat2'])]]
                    }
                }
                # add the feature to the geojson object
                geojson['features'].append(feature)
            # write the geojson object to the geojson file
            json.dump(geojson, geojson_f)

    print('Success: geojson file created')
    sys.exit(0)
