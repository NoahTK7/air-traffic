import threading
import json
import os

input_dir = '../data/raw_json_feb1'
output_dir = '../data/raw_csv_feb1'

def parse(num):
    file_name = f'{num}Z.json'
    with open(f'{input_dir}/{file_name}', 'r') as input_file:
        data = json.load(input_file)
    with open(f'{output_dir}/{file_name.replace(".json", ".csv")}', 'w') as output_file:
        # output_file.write('hex,lat,lon\n') # header
        for plane in data['aircraft']:
            if 'hex' in plane and 'lat' in plane and 'lon' in plane and 'r' in plane and 't' in plane:
                try:
                    output_file.write(f'{round(data["now"])},{plane["hex"]},{plane["lat"]},{plane["lon"]},{plane["r"]},{plane["t"]}\n')
                except KeyError as e:
                    pass
                    # print(e)
                    # print(plane)
                    # continue
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

