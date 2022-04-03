import gzip
import threading
import requests
import os

def download_zip(num):
    print(f'getting \t\t{num}Z.json.gz')
    url = f'https://samples.adsbexchange.com/readsb-hist/2022/02/01/{num}Z.json.gz'
    with requests.get(url, stream=True) as response:
        with open(f'input/{num}Z.json.gz', 'wb') as output_file:
            output_file.write(response.content)

# https://samples.adsbexchange.com/readsb-hist/2022/02/01/235955Z.json.gz

def read_archive(num):
    try:
        with gzip.GzipFile(f'input/{num}Z.json.gz', 'rb', 9) as in_file:
            with open(f'input/{num}Z.json', 'wb') as out_file:
                out_file.write(in_file.read())
                print(f'wrote \t\t\t{num}Z.json')
    except:
        print(f'*error with \t\t{num}Z.json.gz')
    finally:
        os.remove(f'input/{num}Z.json.gz')
        print(f'removed \t\t{num}Z.json.gz')


def main(start, stop):
    for i in range(start, stop, 5):
        if i % 100 >= 60:
            continue
        num = str(i).zfill(6)
        if os.path.exists(f'input/{num}Z.json'):
            print(f'{num}Z.json already exists')
            continue
        download_zip(num)
        read_archive(num)


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

