import threading
import json
import os

START = 0
STOP = 240_000
STEP = 60*5

# OUT_FILE = 'output/output-300.txt'

# files = ['input/000000Z.json', 'input/000005Z.json']
def files(offset=0):
    for i in range(START + offset, STOP, STEP):
        if i % 100 >= 60:
            continue
        num = str(i).zfill(6)
        yield f'input/{num}Z.json'

def process(i):
    offset = i * 5
    dict1 = None
    with open(f'output/output-300-{i}.txt', 'a') as output_file:
        for file in files(offset):
            if not os.path.exists(file):
                dict1 = None
                continue

            with open(file, 'r') as input_file:
                data = json.load(input_file)

            new_dict = dict()
            for plane in data['aircraft']:
                if 'hex' in plane and 'lat' in plane and 'lon' in plane:
                    new_dict[plane['hex']] = (plane['lat'], plane['lon'])

            # if there is a not a previously saved dictionary (first file, or last file was missing),
            # save the newly loaded data and continue to next file
            if dict1 is None:
                dict1 = new_dict
                continue

            # for each key that is in both dict1 and new_dict
            for key in dict1.keys():
                if key in new_dict:
                    # if the key is in both dict1 and new_dict, then print the key and the distance between the two
                    output_file.write(f'{key}|{dict1[key][0]}|{dict1[key][1]}|{new_dict[key][0]}|{new_dict[key][1]}\n')
            print(f'wrote output for {file}')
            dict1 = new_dict

if __name__ == '__main__':
    threads = []
    num_offsets = int(STEP / 5)
    for i in range(0, num_offsets):
        threads.append(threading.Thread(target=process, args=(i,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


