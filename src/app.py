import re
import json
import argparse

from typing import Optional
from mpi4py import MPI

LANG_PATH = "data/language.json"

def grid(grid_file: str) -> dict:
    id_grid = {
        9: "A1", 10: "B1", 11: "C1", 12: "D1",
        13: "A2", 14: "B2", 15: "C2", 16: "D2",
        17: "A3", 18: "B3", 19: "C3", 20: "D3",
        21: "A4", 22: "B4", 23: "C4", 24: "D4"
    }

    syd_grids = []

    with open(grid_file, "r") as f:
        grid = json.load(f)

    for data in grid["features"]:
        feature = {}
        feature["id"] = id_grid[int(data["properties"]["id"])]
        feature["x1"] = data["geometry"]["coordinates"][0][0][0]
        feature["x2"] = data["geometry"]["coordinates"][0][2][0]
        feature["y1"] = data["geometry"]["coordinates"][0][0][1]
        feature["y2"] = data["geometry"]["coordinates"][0][1][1]
        syd_grids.append(feature)

    return syd_grids

def run_app(twitter_file: str, grid_file: str):
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    config = read_config(comm, rank, size, twitter_file)

    syd_grids = grid(grid_file)

    line_start = config.get("line_start")
    chunk_size = config.get("chunk_size")
    num_rows = config.get("num_rows")
    line_end = line_start + chunk_size - 1
    
    if rank == size - 1:
        line_end = num_rows + 1

    print(f"Rank: {rank}, Start at line: {line_start}")

    # TODO: the final version of language_count will look something like
    # {
    #     "A1": {
    #         "english": 10,
    #         "french": 5
    #     },
    #     "A2": {
    #         "chinese": 10,
    #         "french": 1,
    #         "english": 100
    #     }
    # }
    language_count = {}
    with open(twitter_file, "r") as f:
        count = 1
        for line in f:
            
            # Stop if we have reached end of chunk for this core
            if count > line_end:
                break

            if count >= line_start and count <= line_end:
                # TODO: handle language count per grid
                obj = read_twitter_obj(line, syd_grids)
                if obj is not None:
                    lang = obj.get("language", "-")

                    if lang not in language_count:
                        language_count[lang] = 0

                    language_count[lang] += 1

            count += 1

    print(language_count)
    combined = comm.gather(language_count)

    if rank == 0:
        print_report(combined)

def allocate_tweet_to_grid(syd_grids: dict, coordinates: list) -> str:
    #TODO check tweet coordinates to be allocated on which grid
    return "grid"

def print_report(gathered_count: dict):
    print("===== Report =====")
    total_count = {}
    for data in gathered_count:
        for lang in data:
            val = data[lang]
            if lang not in total_count:
                total_count[lang] = 0

            total_count[lang] += val

    for lang in total_count:
        print(f"{lang}: {total_count[lang]}")


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

def read_twitter_obj(line: str, syd_grids: dict) -> Optional[dict]:
    """Read each line of Twitter json file and return a dict object with relevant metadata.

    Args:
        line (str): raw line from input file

    Returns:
        dict: dict with relevant metadata or None
            If language and coordinates are known, returns the following object
            {
                "language": "English",
                "grid": "A1"
            }
            Otherwise returns None
    """
    with open(LANG_PATH, "r") as f:
        lang_mapper = json.load(f)

    line = line.strip()
    if line.endswith("]}"):
        json_str = line.strip("}").strip("]")
    elif line.endswith(","):
        json_str = line.strip(",")
    elif line.endswith("}"):
        json_str = line.strip("}")

    obj = json.loads(json_str)
    tweet_coordinates = obj.get("doc", {}).get("coordinates")

    if tweet_coordinates is None:
        return None

    iso_lang = obj.get("doc", {}).get("metadata", {}).get("iso_language_code")
    language = lang_mapper.get(iso_lang)

    if language is None:
        print(f"ERROR: unknown language code {iso_lang}")
        return None

    # TODO: convert coordinates to grid A1, A2, C2, etc
    grid = allocate_tweet_to_grid(syd_grids, tweet_coordinates)
    if language is not None and grid is not None:
        return {
            "language": language,
            "grid": grid
        }
    else:
        return None

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help="File path to the twitter json file", dest="file_path", required=True)
    parser.add_argument("--grid", help="File path to the grid json file", dest="grid_file_path", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    options = parse_args()
    run_app(options.file_path, options.grid_file_path)
