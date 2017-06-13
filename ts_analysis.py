import datetime
import argparse
import os,sys
import gzip
import numpy as np
from collections import defaultdict
import json


PROCESS_BASELINE = True
PARSELY_BUCKET = "s3://hearstdataservices/buzzing/parselyjson/"
#BASELINE_BUCKET='s3://hearstkinesisdata/processedsparkjsonix/'

dt = datetime.datetime.utcnow()
prev_hour = dt - datetime.timedelta(hours=1)

#minutes_range = ["15","30","45","60"]
#buckets_range = [["05","10","15"],["20","25","30"],["35","40","45"],["50","55","60"]]

def main():
    hour = '{:02d}'.format(prev_hour.hour)
    minutes_range = ["15"]
    buckets_range = [["10"]]


    for i,min  in enumerate(minutes_range):
        for buc in buckets_range[i]:
            month = '{:02d}'.format(dt.month)
            day = '{:02d}'.format(dt.day)
            period_str = "{year}/{month}/{day}/{hour}/{minute}/{bucket}/".format(year=dt.year, month=month, day=day, hour=hour, minute=min, bucket=buc)
            print(period_str)
            hm_str = hour + ":" + min
            hm = datetime.datetime.strptime(hm_str, "%H:%M")

            try:
                test_lines = load_data(PARSELY_BUCKET, period_str, delete=True)
                parsely_start_times = get_start_times(test_lines)

                parsely_diff = []
                for p in parsely_start_times:
                    parsely_diff.append((hm - p).seconds / 60)

                p_diff_max = np.max(parsely_diff)
                alert = False
                if p_diff_max > 20:
                    print("ALERT: Delay in data")
                    alert= True

                return alert, p_diff_max
            except:
                print("CANNOT RETRIEVE DATA FROM {period_str} FROM PARSELY".format(period_str=period_str))
                return False, -1



def get_start_times(lines):
    start_times = defaultdict(int)
    for el in lines:
        st = el["startq"]
        st = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
        start_times[st] += 1
    print(start_times)
    start_times = sorted(start_times)

    return start_times

def load_data(bucket_name, period_str, delete=True):
    try:
        cp_command = 'aws s3 cp ' + bucket_name + period_str + 'part-00000.gz temp.gz'
        print(cp_command)
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
    alert, p_diff_max = main()