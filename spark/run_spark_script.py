import argparse
import subprocess
from datetime import datetime
import sys
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--feed-name', type=str, default="change_relay")
    parser.add_argument('--feed-type', type=str, default="medicalclaims")
    parser.add_argument('--version', type=str, default="")
    parser.add_argument('--script', type=str, default=None)

    args, unknown = parser.parse_known_args()

    try:
        os.mkdir("logs/")
    except:
        pass

    start_date = datetime.strptime(args.start, "%Y-%m-%d") if args.start else None
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.today()
    run_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.today()

    script = args.script
    feed_name = args.feed_name
    feed_type = args.feed_type
    version = args.version
    if start_date:
        file_date = start_date.strftime("%Y%m%d") + "_script.out"
    else:
        file_date = run_date.strftime("%Y%m%d") + "_script.out"

    # command_template = """
    #     nohup /bin/sh -c "PYSPARK_PYTHON=python3 /home/hadoop/spark/bin/run.sh /home/hadoop/spark/providers/{}/{}/normalize.py --date {}" &> logs/{} &
    # """
    command_template = """
        nohup /bin/sh -c "PYSPARK_PYTHON=python3 /home/hadoop/spark/bin/run.sh /home/hadoop/{script} {args}" &> logs/{log_location} &
    """
    command = command_template.format(
        script=script,
        args=" ".join(sys.argv[1:]),
        log_location=file_date
    )
    
    subprocess.call(command, shell=True)
