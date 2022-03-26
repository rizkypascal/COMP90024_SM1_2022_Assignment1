import re
import json
import argparse

from math import ceil
from mpi4py import MPI



def run_app(filename: str):
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    print('My rank is ',rank)

    if rank == 0:
        with open(filename) as f:
            first_line = f.readline().strip()

            # count total number of rows
            regex = r'"total_rows":(\d+)'
            m = re.search(regex, first_line)
            if m and len(m.groups()) >= 1:
                num_rows = int(m.group(1))
                chunk_size = ceil(num_rows / size)
            else:
                raise Exception("Invalid input json file. Number of rows not found.")

            if size == 1:
                data = {
                    "line_start": 2, 
                    "chunk_size": num_rows,
                    "num_rows": num_rows
                }
            else:
                for i in range(size):
                    line_start = 2 + (i * chunk_size)
                    data = {
                        "line_start": line_start, 
                        "chunk_size": chunk_size,
                        "num_rows": num_rows
                    }
                    comm.send(data, dest=i)

    if size > 1:
        data = comm.recv(source=0)

    line_start = data.get("line_start")
    chunk_size = data.get("chunk_size")
    num_rows = data.get("num_rows")
    line_end = line_start + chunk_size - 1
    
    if rank == size - 1:
        line_end = num_rows

    print(f"Rank: {rank}, Start: {line_start}")

    with open(filename, "r") as f:
        count = 1
        for line in f:
            if count >= line_start and count <= line_end:
                print(f"line: {count}")
                line = line.strip()
                if line.endswith("}"):
                    json_str = line.strip("}").strip("]")
                else:
                    json_str = line.strip(",")

                obj = json.loads(json_str)

                print(obj.get("id", "-"))


            count += 1

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help="File path to the twitter json file", dest="file_path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    options = parse_args()
    run_app(options.file_path)
