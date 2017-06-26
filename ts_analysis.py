import datetime
import argparse
import os,sys
import gzip
import numpy as np
from collections import defaultdict
import json
from config import DEV_INTERMEDIATE_OUTPUT_LOC

PROCESS_BASELINE = True



dt = datetime.datetime.utcnow()
prev_hour = dt - datetime.timedelta(hours=1)

def main():
    hour = '{:02d}'.format(prev_hour.hour)
    minutes_range = ["15"]
    buckets_range = [["10"]]


    for i,min  in enumerate(minutes_range):
        for buc in buckets_range[i]:
            month = '{:02d}'.format(dt.month)
            day = '{:02d}'.format(dt.day)
            period_str = "{year}/{month}/{day}/{hour}/{minute}/{bucket}/".format(year=dt.year, month=month, day=day, hour=hour, minute=min, bucket=buc)
            hm_str = hour + ":" + min
            hm = datetime.datetime.strptime(hm_str, "%H:%M")

            try:
                test_lines = load_data(DEV_INTERMEDIATE_OUTPUT_LOC, period_str, delete=True)
                parsely_start_times = get_start_times(test_lines)

                parsely_diff = []
                for p in parsely_start_times.keys():
                    parsely_diff.append((hm - p).seconds / 60)

                p_diff_max = np.max(parsely_diff)
                A = dict((datetime.datetime.strftime(key,"%Y-%m-%d %H:%M:%S"), value) for (key, value) in parsely_start_times.items())

                A["maximum difference in timestamps:"]="%.2f minutes"%p_diff_max
                print(A)
                if p_diff_max > 20:
                    return "ALERT: {delay} min delay in incoming data stream".format(delay=p_diff_max), A
                print("OK")
                return "OK", A

            except Exception as e:
                print(e.args)
                print("CANNOT RETRIEVE DATA FROM {period_str} FROM PARSELY".format(period_str=period_str))
                return "ALERT: Cannot retrieve data", {}



def get_start_times(lines):
    start_times = defaultdict(int)
    for el in lines:
        st = el["startq"]
        st = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
        start_times[st] += 1
    #start_times = sorted(start_times)

    return start_times

def load_data(bucket_name, period_str, delete=True):
    try:
        cp_command = 'aws s3 cp ' + bucket_name + period_str + 'part-00000.gz temp.gz'
        os.system(cp_command)
        with gzip.open("temp.gz", 'rb') as infile:

            test_lines = [json.loads(el) for el in infile.readlines()]
        if delete:
            os.remove("temp.gz")
        return test_lines
    except:
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    args = parser.parse_args()
    main()
