import re
import json
import argparse

from typing import Optional
from mpi4py import MPI

LANG_PATH = "data/language.json"

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
        coordinates = simplify_coordinates(data["geometry"]["coordinates"])
        feature["x1"] = coordinates["x1"]
        feature["x2"] = coordinates["x2"]
        feature["y1"] = coordinates["y1"]
        feature["y2"] = coordinates["y2"]
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
            if count >= line_start and count <= line_end:
                # TODO: handle language count per grid
                obj = read_twitter_obj(line, syd_grids)
                if obj is not None:
                    lang = obj.get("language", "-")
                    print(f'Line {count}: {lang}')

                    if lang not in language_count:
                        language_count[lang] = 0

                    language_count[lang] += 1

            count += 1

    combined = comm.gather(language_count)

    if rank == 0:
        print_report(combined)

def allocate_tweet_to_grid(syd_grids: dict, coordinates: list) -> Optional[str]:
    #TODO not yet handle edge case such as tweet vertices overlap multicells
    """To allocate tweet coordinates into grid

    These rules must be applied whether the tweets are assignable to the cell or not:
        - tweet(x1,x2))(y1,y2) inclusive in cell(x1, x2)(y1, y2) (in the cell) (cond1)
        = tweet(x2) is not in cell coordinates, others all in cell coordinates (overlapping y axis) (cond2)
        - tweet(x2) is not in cell coordinates, others all in cell coordinates (overlapping x axis) (cond3)
        - at least one x axis and y axis (2 points) must be exist in cell, with conditions: (cond4)
            - if only intersect with one cell, then assign that cell
            - if intersect with 2 cells, then check which axis on the same position,
              then take cell with another axis on min value (eg: exist in A1 and A2 with same y axis value, therefore take the x axis min value) (cond5)
            - if intersect more than 2 cells, exclude it (cond6) 
    """
    grid = None
    two_points_coordinate = {
        "x1y1": None,
        "x1y2": None,
        "x2y1": None,
        "x2y2": None
    }
    two_points_count = 0
    tweet_coordinates = simplify_coordinates(coordinates)

    for i in syd_grids:
        # cond1
        if (tweet_coordinates["x1"] >= i["x1"] and tweet_coordinates["x1"] < i["x2"]) \
        and (tweet_coordinates["x2"] > i["x1"] and tweet_coordinates["x2"] <= i["x2"]) \
        and (tweet_coordinates["y1"] >= i["y1"] and tweet_coordinates["y1"] < i["y2"]) \
        and (tweet_coordinates["y2"] > i["y1"] and tweet_coordinates["y2"] <= i["y2"]):
            return i["id"]
        
        # cond2
        elif (tweet_coordinates["x1"] >= i["x1"] and tweet_coordinates["x1"] < i["x2"]) \
        and (tweet_coordinates["x2"] <= i["x1"] or tweet_coordinates["x2"] > i["x2"]) \
        and (tweet_coordinates["y1"] >= i["y1"] and tweet_coordinates["y1"] < i["y2"]) \
        and (tweet_coordinates["y2"] > i["y1"] and tweet_coordinates["y2"] <= i["y2"]):
            return i["id"]
        
        # cond3
        elif (tweet_coordinates["x1"] >= i["x1"] and tweet_coordinates["x1"] < i["x2"]) \
        and (tweet_coordinates["x2"] > i["x1"] and tweet_coordinates["x2"] <= i["x2"]) \
        and (tweet_coordinates["y1"] >= i["y1"] and tweet_coordinates["y1"] < i["y2"]) \
        and (tweet_coordinates["y2"] <= i["y1"] or tweet_coordinates["y2"] > i["y2"]):
            return i["id"]
        
        # cond2 but still loop to check if other side of x axis exist
        elif (tweet_coordinates["x1"] < i["x1"] or tweet_coordinates["x1"] >= i["x2"]) \
        and (tweet_coordinates["x2"] > i["x1"] and tweet_coordinates["x2"] <= i["x2"]) \
        and (tweet_coordinates["y1"] >= i["y1"] and tweet_coordinates["y1"] < i["y2"]) \
        and (tweet_coordinates["y2"] > i["y1"] and tweet_coordinates["y2"] <= i["y2"]):
            grid = i["id"]
        
        # cond2 but still loop to check if other side of y axis exist
        elif (tweet_coordinates["x1"] >= i["x1"] and tweet_coordinates["x1"] < i["x2"]) \
        and (tweet_coordinates["x2"] > i["x1"] and tweet_coordinates["x2"] <= i["x2"]) \
        and (tweet_coordinates["y1"] < i["y1"] or tweet_coordinates["y1"] >= i["y2"]) \
        and (tweet_coordinates["y2"] > i["y1"] and tweet_coordinates["y2"] <= i["y2"]):
            grid = i["id"]

        # cond4
        elif (tweet_coordinates["x1"] < i["x1"] and tweet_coordinates["x1"] >= i["x2"]) \
        and (tweet_coordinates["x2"] <= i["x1"] or tweet_coordinates["x2"] > i["x2"]) \
        and (tweet_coordinates["y1"] < i["y1"] or tweet_coordinates["y1"] >= i["y2"]) \
        and (tweet_coordinates["y2"] > i["y1"] and tweet_coordinates["y2"] <= i["y2"]):
            two_points_count += 1
            two_points_coordinate["x1y2"] = i["id"]
        
        # cond4
        elif (tweet_coordinates["x1"] >= i["x1"] and tweet_coordinates["x1"] < i["x2"]) \
        and (tweet_coordinates["x2"] <= i["x1"] or tweet_coordinates["x2"] > i["x2"]) \
        and (tweet_coordinates["y1"] < i["y1"] and tweet_coordinates["y1"] >= i["y2"]) \
        and (tweet_coordinates["y2"] <= i["y1"] or tweet_coordinates["y2"] > i["y2"]):
            two_points_count += 1
            two_points_coordinate["x1y1"] = i["id"]
        
        # cond4
        elif (tweet_coordinates["x1"] < i["x1"] or tweet_coordinates["x1"] >= i["x2"]) \
        and (tweet_coordinates["x2"] > i["x1"] and tweet_coordinates["x2"] <= i["x2"]) \
        and (tweet_coordinates["y1"] < i["y1"] and tweet_coordinates["y1"] >= i["y2"]) \
        and (tweet_coordinates["y2"] <= i["y1"] or tweet_coordinates["y2"] > i["y2"]):
            two_points_count += 1
            two_points_coordinate["x2y1"] = i["id"]
        
        # cond4
        elif (tweet_coordinates["x1"] < i["x1"] or tweet_coordinates["x1"] >= i["x2"]) \
        and (tweet_coordinates["x2"] > i["x1"] and tweet_coordinates["x2"] <= i["x2"]) \
        and (tweet_coordinates["y1"] < i["y1"] or tweet_coordinates["y1"] >= i["y2"]) \
        and (tweet_coordinates["y2"] > i["y1"] and tweet_coordinates["y2"] <= i["y2"]):
            two_points_count += 1
            two_points_coordinate["x2y2"] = i["id"]

        # cond6
        if(two_points_count > 2):
            return None

    if(grid is not None):
        return grid
    else:
        # cond5
        if(two_points_coordinate["x1y2"] is not None and two_points_coordinate["x2y2"] is not None):
            return two_points_coordinate["x1y2"]
        elif(two_points_coordinate["x2y1"] is not None and two_points_coordinate["x2y2"] is not None):
            return two_points_coordinate["x2y1"]
        else:
            if(two_points_coordinate["x1y1"] is not None and two_points_coordinate["x1y2"] is None and two_points_coordinate["x2y1"] is None and two_points_coordinate["x2y2"] is None):
                return two_points_coordinate["x1y1"]
            elif(two_points_coordinate["x1y1"] is None and two_points_coordinate["x1y2"] is not None and two_points_coordinate["x2y1"] is None and two_points_coordinate["x2y2"] is None):
                return two_points_coordinate["x1y2"]
            elif(two_points_coordinate["x1y1"] is None and two_points_coordinate["x1y2"] is None and two_points_coordinate["x2y1"] is not None and two_points_coordinate["x2y2"] is None):
                return two_points_coordinate["x2y1"]
            elif(two_points_coordinate["x1y1"] is None and two_points_coordinate["x1y2"] is None and two_points_coordinate["x2y1"] is None and two_points_coordinate["x2y2"] is not None):
                return two_points_coordinate["x2y2"]

    return None

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
    if line.endswith("}"):
        json_str = line.strip("}").strip("]")
    else:
        json_str = line.strip(",")

    obj = json.loads(json_str)

    try:
        tweet_coordinates = obj.get("doc", {}).get("place", {}).get("bounding_box", {}).get("coordinates")
    except AttributeError:
        return None

    iso_lang = obj.get("doc", {}).get("metadata", {}).get("iso_language_code")
    language = lang_mapper[iso_lang]

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
