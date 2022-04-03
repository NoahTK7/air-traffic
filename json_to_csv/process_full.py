import threading
import json

input_dir = '../data/raw_json_feb1'
output_dir = '../data/raw_csv_feb1_full'

keys = ['hex', 'type', 'flight', 'r', 't', 'dbFlags', 'alt_baro', 'alt_geom', 'gs', 'ias', 'tas', 'mach', 'track', 'track_rate', 'roll', 'mag_heading', 'true_heading', 'baro_rate', 'geom_rate', 'squawk', 'rr_lat', 'rr_lon', 'emergency', 'category', 'nav_qnh', 'nav_altitude_mcp', 'nav_altitude_fms', 'nav_heading', 'nav_modes', 'lat', 'lon', 'nic', 'rc', 'seen_pos', 'track', 'version', 'nic_baro', 'nac_p', 'nac_v', 'sil', 'sil_type', 'gva', 'sda', 'mlat', 'tisb', 'messages', 'seen', 'rssi', 'alert', 'spi', 'wd', 'ws', 'oat', 'tat']

def parse(num):
    file_name = f'{num}Z.json'
    with open(f'{input_dir}/{file_name}', 'r') as input_file:
        data = json.load(input_file)
    with open(f'{output_dir}/{file_name.replace(".json", ".csv")}', 'w') as output_file:
        # output_file.write('hex,lat,lon\n') # header
        for plane in data['aircraft']:
            current_line = ""
            # for each key in plane
            for key in keys:
                try:
                    val = plane[key]
                    current_line += f'{val},'
                except KeyError:
                    current_line += ','
            current_line = current_line[:-1] + '\n'
            output_file.write(current_line)
    print(f'wrote output for {file_name}')


# for each json file in input_dir, parse the json
# and write the data to a csv file
def main(start, stop):
    for i in range(start, stop, 5):
        if i % 100 >= 60:
            continue
        num = str(i).zfill(6)
        parse(num)


if __name__ == '__main__':
    threads = []
    threads.append(threading.Thread(target=main, args=(0, 30_000)))
    threads.append(threading.Thread(target=main, args=(30_000, 60_000)))
    threads.append(threading.Thread(target=main, args=(60_000, 90_000)))
    threads.append(threading.Thread(target=main, args=(90_000, 120_000)))
    threads.append(threading.Thread(target=main, args=(120_000, 150_000)))
    threads.append(threading.Thread(target=main, args=(150_000, 180_000)))
    threads.append(threading.Thread(target=main, args=(180_000, 210_000)))
    threads.append(threading.Thread(target=main, args=(210_000, 240_000)))

    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()

