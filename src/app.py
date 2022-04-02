import os
import time
import json
import csv
import argparse

from typing import Optional, List
from mpi4py import MPI

current_dir = os.path.dirname(os.path.abspath(__file__))
LANG_PATH = os.path.join(current_dir, "..", "data", "language.json")

LANG_CODE_UNDEFINED = "und"


def run_app(twitter_file: str, grid_file: str, lang_map_file: str):
    """Main function to identify grid of a location point in a Tweet.

    Args:
        twitter_file (str): path to twitter json file
        grid_file (str): path to grid json file
        lang_map_file (str): path to language map file
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    start = MPI.Wtime()

    config = read_config(comm, grid_file, lang_map_file)

    # Main logic to process twitter file
    language_count = read_twitter_file(twitter_file, config)

    # Combine results from all processes
    combined = comm.gather(language_count)

    if rank == 0:
        sorted_count = count_and_sort(combined)
        print_report(sorted_count, start)
    else:
        print(f"Rank {rank}: completed task in {MPI.Wtime() - start} seconds")


def read_twitter_file(twitter_file: str, config: dict) -> dict:
    """Read Twitter json file and process lines according to config.

    Args:
        twitter_file (str): file path to Twiter json file
        config (dict): instructions on how to read the file. it consists of line start and step to skip.
        grids (dict): a dictionary of grids to be mapped to
        lang_mapper (dict): a dictionary to map language code to language name

    Returns:
        dict: a dictionary of language count keyd by grid name
        Example format of the variable language_count 
        {
            "A1": {
                "english": 10,
                "french": 5
            },
            "A2": {
                "chinese": 10,
                "french": 1,
                "english": 100
            }
        }
    """
    line_start = config.get("line_start")
    step = config.get("step")
    grids = config.get("grids")
    lang_map = config.get("lang_map")

    language_count = {}
    with open(twitter_file, "r") as f:
        count = 1
        next_line = line_start

        for line in f:
            
            if count == next_line:
                obj = read_twitter_obj(line, grids, lang_map)
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
                next_line += step

            count += 1

    return language_count


def simplify_coordinates(coordinates: list) -> dict:
    """
        To convert polygon coordinates to
        x1(xmin), x2(xmax), y1(ymin), y2(ymax)

    Args:
        coordinates (list):  A list of grid coordinates that forms a rectangle polygon

    Returns:
        dict: a dictionary of the grid coordinates keyed by x1, x2, y1, y2
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
    grids = {}

    with open(grid_file, "r") as f:
        grid = json.load(f)

    for data in grid.get("features", []):
        coordinates = simplify_coordinates(data["geometry"]["coordinates"])
        min_x = coordinates["x1"]
        min_y = coordinates["y1"]
        if min_y not in grids:
            grids[min_y] = {}

        grids[min_y][min_x] = {
            "x1": coordinates["x1"],
            "x2": coordinates["x2"],
            "y1": coordinates["y1"],
            "y2": coordinates["y2"]
        }

    # sort by x
    for y in grids:
        grids[y] = {k: v for k, v in sorted(grids[y].items(), key=lambda item: item[0])}

    # sort by y DESC
    sorted_grids = {k: v for k, v in sorted(grids.items(), key=lambda item: item[0], reverse=True)}

    # Now automatically assign grid name where
    # the smallest row starts with A and 
    # the smallest column starts with 1
    grids_by_name = {}
    row_count = 1
    for row in sorted_grids:
        row_name = row_num_to_row_name(row_count)

        col_count = 1
        for col in sorted_grids[row]:
            grid_name = f"{row_name}{col_count}"
            grids_by_name[grid_name] = sorted_grids[row][col]

            col_count += 1

        row_count += 1

    return grids_by_name

def row_num_to_row_name(row_num: int):
    """convert int to letter(s) that represent column name.

    Args:
        row_num (int): starts from 0
    """
    first_letter = 'A'
    first_ascii = ord(first_letter)
    num_unique_letters = 26

    # initialize output string as empty
    col_name = ''
 
    while row_num > 0:
        index = int((row_num - 1) % num_unique_letters)
        col_name += chr(index + first_ascii)
        row_num = (row_num - 1) // num_unique_letters
 
    return col_name[::-1]


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


def count_and_sort(gathered_count: List[dict]) -> dict:
    """Sort and count the total number of tweets and languages in each grid.

    Args:
        gathered_count (List[dict]):  A list of dictionary where the
            dictionary is the summary of language count per grid in each process 

    Returns:
        dict: Dictionary keyed by grid name
    """
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

    # Sort the result of each grid by most language count and then by language name alphabetically
    sorted_count = {}
    for grid in total_count:
        # sort by language alphabetically
        sorted_count[grid] = {k: v for k, v in sorted(total_count[grid].items(), key= lambda item: item[0])}

        # Sort by most language count
        sorted_count[grid] = {k: v for k, v in sorted(sorted_count[grid].items(), key=lambda item: item[1], reverse=True)}


    # Sort by grid name
    sorted_count = {k: v for k, v in sorted(sorted_count.items(), key=lambda item: item[0])}

    overall_count = {}
    for grid in sorted_count:
        overall_count[grid] = {
            "total_tweets": sum(val for val in sorted_count[grid].values()),
            "total_lang": len(sorted_count[grid].keys()),
            "languages": sorted_count[grid]
        }
        
    return overall_count


def print_report(summary: dict, start_time: float):
    """Print out the language and tweet count summary per grid

    Args:
        summary (dict): language and tweet count summary per grid
    """
    delimiter = "\t"
    top_count = 10
    cols = {"Cell": 4, "#Total Tweets": 14, "#Number of Languages Used": 26, "#Top 10 Languages & #Tweets": 28}
    
    timestamp = int(time.time())
    output_file = f"language_count_{timestamp}.txt"

    header = [name.center(width) for name, width in cols.items()]

    csv_writer = None
    with open(output_file, "w") as f:
        csv_writer = csv.writer(f, delimiter=delimiter)
        csv_writer.writerow(header)

        print("------------------------------- Report -------------------------------")
        print(delimiter.join(header))
        for grid in summary:
            col_widths = [w for w in cols.values()]
            lang_summary = [f"{lang}-{count}" for lang, count in summary[grid]["languages"].items()]
            row = [
                grid.ljust(col_widths[0]),
                str(summary[grid]["total_tweets"]).ljust(col_widths[1]),
                str(summary[grid]["total_lang"]).ljust(col_widths[2]),
                ", ".join(lang_summary[:top_count]).ljust(col_widths[3])
            ]

            print(delimiter.join(row))
            csv_writer.writerow(row)

    print("----------------------------------------------------------------------")
    print(f"Output file: {output_file}")
    print(f"Elapsed time: {MPI.Wtime() - start_time} seconds")
    print("----------------------------------------------------------------------")

def read_config(comm, grid_file: str, lang_map_file: str) -> dict:
    """Read the config for this process.

    Args:
        comm (_type_): MPI comm

    Returns:
        dict: a dictionary consists of information each process needs to run
    """
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
        grids = load_grids(grid_file)

        with open(lang_map_file, "r") as f:
            lang_mapper = json.load(f)

        if size == 1:
            config = {
                "line_start": 2,
                "step": 1,
                "grids": grids,
                "lang_map": lang_mapper
            }
        else:
            line_start = 2

            # We want each process to parse alternate line
            for i in range(size):
                config = {
                    "line_start": line_start + i, 
                    "step": size,
                    "grids": grids,
                    "lang_map": lang_mapper
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
    # Parse a line of JSON
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

    # Skip undefined language
    if iso_lang == LANG_CODE_UNDEFINED:
        return None

    # Fron language code to language name
    language = lang_mapper.get(iso_lang)
    
    if language is None:
        print(f"ERROR: unknown language code {iso_lang}")
        return None

    # Identify if tweet location falls into one of the grids A1, A2, B1, etc
    grid = identify_grid(grids, tweet_point)
    if language is not None and grid is not None:
        return {
            "language": language,
            "grid": grid
        }
    else:
        return None

def parse_args():
    """Parse command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help="File path to the twitter json file", dest="twitter_file_path", required=True)
    parser.add_argument("--grid", help="File path to the grid json file", dest="grid_file_path", required=True)
    parser.add_argument(
        "--lang-map", help="File path to a json file that provides language iso code to name map", 
        dest="lang_map_file_path", default=LANG_PATH
    )
    return parser.parse_args()


if __name__ == "__main__":
    options = parse_args()

    twitter_file_path = os.path.abspath(options.twitter_file_path)
    grid_file_path = os.path.abspath(options.grid_file_path)
    lang_map_file_path = os.path.abspath(options.lang_map_file_path)

    if not os.path.exists(twitter_file_path):
        print(f"ERROR: Grid file {twitter_file_path} does not exist.")
        exit(1)

    if not os.path.exists(grid_file_path):
        print(f"ERROR: Grid file {grid_file_path} does not exist.")
        exit(1)

    if not os.path.exists(lang_map_file_path):
        print(f"ERROR: Language file {lang_map_file_path} does not exist.")
        exit(1)
    
    run_app(twitter_file_path, grid_file_path, lang_map_file_path)
