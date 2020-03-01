import csv
import sys
import progressbar
import yaml
import argparse

def step(params, config):
    input_file = config["input_file"]
    output_dir = config["output_dir"]
    split_index = config["split_index"]

    with open(input_file) as f:
        n = sum(1 for i in f) - 1

    file_dict = {}

    bar = progressbar.ProgressBar(max_value=n, redirect_stdout=True)

    with open(input_file, newline="") as f:
        r = csv.reader(f)
        headers = next(r)
        pni = headers.index(split_index)
        i = 0
        for row in r:
            pn = row[pni]
            _, w = file_dict.get(pn, (None, None))
            if w is None:
                filename = f"{output_dir}/{pn}"
                print(f"opening file {filename} for output")
                f = open(f"{output_dir}/{pn}", "w+", newline="")
                w = csv.writer(f)
                file_dict[pn] = (f, w)
                w.writerow(headers)
            w.writerow(row)
            i += 1
            bar.update(i)

    for f, _ in progressbar.progressbar(file_dict.values()):
        f.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--config', type=str, required=True, help='sum the integers (default: find the max)')

    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.load(f)
    step({}, config)

