import argparse
import csv
import json

def main(infile, outfile):
    with open(infile, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with open(outfile, 'w') as f:
        json.dump(rows, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str)
    parser.add_argument('--outfile', type=str)
    args = parser.parse_args()
    main(args.infile, args.outfile)
