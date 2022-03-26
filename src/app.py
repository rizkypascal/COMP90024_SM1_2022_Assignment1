import re
import json
import argparse

from mpi4py import MPI


def run_app(filename: str):
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    config = read_config(comm, rank, size, filename)

    line_start = config.get("line_start")
    chunk_size = config.get("chunk_size")
    num_rows = config.get("num_rows")
    line_end = line_start + chunk_size - 1
    
    if rank == size - 1:
        line_end = num_rows + 1

    print(f"Rank: {rank}, Start at line: {line_start}")

    with open(filename, "r") as f:
        count = 1
        for line in f:
            if count >= line_start and count <= line_end:
                obj = read_twitter_obj(line)
                print(f'Line {count}: {obj.get("id", "-")}')

            count += 1

def read_config(comm, rank: int, size: int, filename: str) -> dict:
    """Read the config for this core.

    Args:
        comm (_type_): _description_
        rank (int): _description_
        size (int): _description_
        filename (str): _description_

    Raises:
        Exception: _description_

    Returns:
        dict: a dictionary consists of 
    """
    if rank == 0:
        with open(filename) as f:
            first_line = f.readline().strip()

            # count total number of rows
            regex = r'"total_rows":(\d+)'
            m = re.search(regex, first_line)
            if m and len(m.groups()) >= 1:
                num_rows = int(m.group(1)) - 1
            else:
                raise Exception("Invalid input json file. Number of rows not found.")

            if size == 1:
                config = {
                    "line_start": 2, 
                    "chunk_size": num_rows,
                    "num_rows": num_rows
                }
            else:
                line_start = 2
                remainder = num_rows % size

                for i in range(size):
                    chunk_size = int(num_rows / size)

                    if i < remainder:
                        chunk_size += 1

                    config = {
                        "line_start": line_start, 
                        "chunk_size": chunk_size,
                        "num_rows": num_rows
                    }
                    comm.send(config, dest=i)

                    line_start += chunk_size

    if size > 1:
        config = comm.recv(source=0)

    return config

def read_twitter_obj(line: str) -> dict:
    """Read each line of Twitter json file and return a dict object.

    Args:
        line (str): raw line from input file

    Returns:
        dict: Twitter object
    """
    line = line.strip()
    if line.endswith("}"):
        json_str = line.strip("}").strip("]")
    else:
        json_str = line.strip(",")

    obj = json.loads(json_str)

    return obj


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help="File path to the twitter json file", dest="file_path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    options = parse_args()
    run_app(options.file_path)
