import os
import re
import json
import argparse

from typing import Optional
from mpi4py import MPI

current_dir = os.path.dirname(os.path.abspath(__file__))
LANG_PATH = os.path.join(current_dir, "..", "data", "language.json")

LANG_CODE_UNDEFINED = "und"

def simplify_coordinates(coordinates: list) -> dict:
    """
        To convert polygon coordinates to
        x1(xmin), x2(xmax), y1(ymin), y2(ymax)
    """
    new_coordinates = {
        "x1": 0.0, "x2": 0.0,
        "y1": 0.0, "y2": 0.0
    }
    new_coordinates["x1"] = new_coordinates["x2"] = coordinates[0][0][0]
    new_coordinates["y1"] = new_coordinates["y2"] = coordinates[0][0][1]

    for i in coordinates[0][1:]:
        if new_coordinates["x1"] < i[0]:
            new_coordinates["x2"] = i[0]
        else:
            new_coordinates["x1"] = i[0]
        
        if new_coordinates["y1"] < i[1]:
            new_coordinates["y2"] = i[1]
        else:
            new_coordinates["y1"] = i[1]
    
    return new_coordinates

def load_grids(grid_file: str) -> dict:
    """Read json file containing grid information.

    Args:
        grid_file (str): file path of the json grid file

    Returns:
        dict: a dictionary of the grid coordinates keyed by grid name
    """
    id_grid = {
        9: "A1", 10: "B1", 11: "C1", 12: "D1",
        13: "A2", 14: "B2", 15: "C2", 16: "D2",
        17: "A3", 18: "B3", 19: "C3", 20: "D3",
        21: "A4", 22: "B4", 23: "C4", 24: "D4"
    }

    grids = {}

    with open(grid_file, "r") as f:
        grid = json.load(f)

    for data in grid["features"]:
        coordinates = simplify_coordinates(data["geometry"]["coordinates"])
        grids[id_grid[int(data["properties"]["id"])]] = {
            "x1": coordinates["x1"],
            "x2": coordinates["x2"],
            "y1": coordinates["y1"],
            "y2": coordinates["y2"]
        }

    return grids

def run_app(twitter_file: str, grid_file: str, lang_map_file: str):
    """Main function to 

    Args:
        twitter_file (str): path to twitter json file
        grid_file (str): path to grid json file
        lang_map_file (str): path to language map file
    """
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    start = MPI.Wtime()

    config = read_config(comm, rank, size, twitter_file)

    syd_grids = load_grids(grid_file)

    with open(lang_map_file, "r") as f:
        lang_mapper = json.load(f)

    line_start = config.get("line_start")
    num_rows = config.get("num_rows")

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
        next_line = line_start

        for line in f:
            
            if count == next_line:
                obj = read_twitter_obj(line, syd_grids, lang_mapper)
                if obj is not None:
                    lang = obj.get("language")
                    grid_code = obj.get("grid")

                    if grid_code is not None and lang is not None:

                        if grid_code not in language_count:
                            language_count[grid_code] = {}

                        if lang not in language_count[grid_code]:
                            language_count[grid_code][lang] = 0

                        language_count[grid_code][lang] += 1
                
                # Each process will parse alternate line
                next_line += size

            count += 1

    combined = comm.gather(language_count)

    if rank == 0:
        sorted_count = count_and_sort(combined)
        print_report(sorted_count)
        print(f"Elapsed time: {MPI.Wtime() - start} seconds")
    else:
        print(f"Rank {rank}: completed task in {MPI.Wtime() - start} seconds")


def identify_grid(grids: dict, tweet_point: list) -> Optional[str]:
    """To allocate tweet coordinates into grid.

    Args:
        grids (dict): Coordinates of grids keyed by its grid name 
        tweet_point (list): a point to represent the coordinates where a tweet is tweeted from

    Returns:
        Optional[str]: grid name of where a Tweet is found or None if it is not in any of the grids.
    """
    grid_min_x = None
    grid_min_y = None
    for grid_id in grids:
        grid_coordinates = grids[grid_id]

        # Identify the left most grid coordinate
        if grid_min_x is None or grid_coordinates["x1"] < grid_min_x:
            grid_min_x = grid_coordinates["x1"]

         # Identify the bottom most grid coordinate
        if grid_min_y is None or grid_coordinates["y1"] < grid_min_y:
            grid_min_y = grid_coordinates["y1"]


        if tweet_point["x"] > grid_coordinates["x1"] \
            and tweet_point["x"] <= grid_coordinates["x2"] \
            and tweet_point["y"] >= grid_coordinates["y1"] \
            and tweet_point["y"] < grid_coordinates["y2"]:

            return grid_id

    # Handle points sitting on left most grid border and bottom most grid border
    if tweet_point["x"] == grid_min_x:
        for grid_id in grids:
            if tweet_point["x"] == grid_coordinates["x1"] \
                and tweet_point["y"] >= grid_coordinates["y1"] \
                and tweet_point["y"] < grid_coordinates["y2"]:

                return grid_id

    if tweet_point["y"] == grid_min_y:
        for grid_id in grids:
            if tweet_point["y"] == grid_coordinates["y1"] \
                and tweet_point["x"] > grid_coordinates["x1"] \
                and tweet_point["x"] <= grid_coordinates["x2"]:

                return grid_id


def count_and_sort(gathered_count: dict):
    total_count = {}
    for data in gathered_count:
        for grid in data:
            if grid not in total_count:
                total_count[grid] = {}

            lang_count = data[grid]

            for lang in lang_count:
                val = lang_count[lang]
                if lang not in total_count[grid]:
                    total_count[grid][lang] = 0

                total_count[grid][lang] += val

    sorted_count = {}
    for grid in total_count:
        sorted_count[grid] = {k: v for k, v in sorted(total_count[grid].items(), key=lambda item: item[1], reverse=True)}

    sorted_count = {k: v for k, v in sorted(sorted_count.items(), key=lambda item: item[0])}

    overall_count = {}
    for grid in sorted_count:
        overall_count[grid] = {
            "total_tweets": sum(val for val in sorted_count[grid].values()),
            "total_lang": len(sorted_count[grid].keys()),
            "languages": sorted_count[grid]
        }
        
    return overall_count


def print_report(summary: dict):
    delimiter = "\t"
    top_count = 10
    cols = {"Cell": 4, "#Total Tweets": 14, "#Number of Languages Used": 26, "#Top 10 Languages & # Tweets": 28}
    
    header = [name.center(width) for name, width in cols.items()]

    print("===== Report =====")
    print(delimiter.join(header))
    for grid in summary:
        # print(f"{grid}: {summary[grid]}")
        col_widths = [w for w in cols.values()]
        lang_summary = [f"{lang}-{count}" for lang, count in summary[grid]["languages"].items()]
        row = [
            grid.ljust(col_widths[0]),
            str(summary[grid]["total_tweets"]).ljust(col_widths[1]),
            str(summary[grid]["total_lang"]).ljust(col_widths[2]),
            ", ".join(lang_summary[:top_count]).ljust(col_widths[3])
        ]

        print(delimiter.join(row))

    print("==================")

def read_config(comm, rank: int, size: int, filename: str) -> dict:
    """Read the config for this process.

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
                    "num_rows": num_rows
                }
            else:
                line_start = 2

                for i in range(size):
                    config = {
                        "line_start": line_start + i, 
                        "num_rows": num_rows
                    }
                    comm.send(config, dest=i)

    if size > 1:
        config = comm.recv(source=0)

    return config

def read_twitter_obj(line: str, grids: dict, lang_mapper: dict) -> Optional[dict]:
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
    

    line = line.strip()
    if line.endswith("]}"):
        json_str = line.strip("}").strip("]")
    elif line.endswith(","):
        json_str = line.strip(",")
    else:
        json_str = line

    # empty line
    if not json_str:
        return None

    obj = json.loads(json_str)

    try:
        tweet_coordinates = obj.get("doc", {}).get("coordinates", {}).get("coordinates")
        tweet_point = {
            "x": tweet_coordinates[0],
            "y": tweet_coordinates[1]
        }
    except AttributeError:
        return None

    iso_lang = obj.get("doc", {}).get("lang")

    if iso_lang == LANG_CODE_UNDEFINED:
        return None

    language = lang_mapper.get(iso_lang)
    
    if language is None:
        print(f"ERROR: unknown language code {iso_lang}")
        return None

    # grid = allocate_tweet_to_grid(grids, tweet_coordinates)
    grid = identify_grid(grids, tweet_point)
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
    parser.add_argument(
        "--lang-map", help="File path to a json file that provides language iso code to name map", 
        dest="lang_map_file_path", default=LANG_PATH
    )
    return parser.parse_args()


if __name__ == "__main__":
    options = parse_args()
    
    run_app(options.file_path, options.grid_file_path, options.lang_map_file_path)
