import datetime
import argparse
import os
import gzip
from collections import defaultdict, OrderedDict
import json
from config import DEV_INTERMEDIATE_OUTPUT_LOC
import pandas as pd
import math
from helpers.boto_connector import BotoConnector
from helpers.redshift_connector import RedshiftConnector

boto_wrapper = BotoConnector(bucket="hearstdataservices", keypath="suguroglu/metrics/")

R = RedshiftConnector()
buckets = [15, 30, 45, 60]
PROCESS_BASELINE = True
THRESHOLD_MINUTES = 20

def ceil_dt(dt, delta):
    return dt + (datetime.datetime.min - dt) % delta


def get_string_buckets(dt):
    bucket_num = math.ceil(dt.minute / 15) - 1
    if bucket_num > -1:
        return dt.hour, buckets[bucket_num], dt.minute
    else:
        return dt.hour - 1, buckets[3], 60


def get_bucket_string(dt):
    h, m, b = get_string_buckets(dt)
    month = '{:02d}'.format(dt.month)
    day = '{:02d}'.format(dt.day)
    h = '{:02d}'.format(h)
    b = '{:02d}'.format(b)
    m = '{:02d}'.format(m)
    period_str = "{year}/{month}/{day}/{hour}/{minute}/{bucket}/".format(year=dt.year, month=month, day=day, hour=h,
                                                                         minute=m, bucket=b)
    return period_str


def get_hm_and_period_str(minutes_ago=5):
    prev_bucket = datetime.datetime.utcnow() - datetime.timedelta(minutes=minutes_ago)
    print(prev_bucket)
    dt_rounded = ceil_dt(prev_bucket, datetime.timedelta(minutes=5))
    period_str = get_bucket_string(dt_rounded)
    h, _, min = get_string_buckets(dt_rounded)

    if min != 60:
        hm_str = "{h}:{m}".format(h=h, m=min)
        hm = datetime.datetime.strptime(hm_str, "%H:%M")
    else:
        hm_str = "{h}:{m}".format(h=h+1, m="00")
        hm = datetime.datetime.strptime(hm_str, "%H:%M")

    bucket_str = datetime.datetime.strftime(dt_rounded,"%Y-%m-%d %H:%M")
    return hm, period_str,bucket_str


def main(is_redshift=False):
    try:
        hm, period_str,bucket_str = get_hm_and_period_str()
        print(period_str)
        print(hm)
        print(bucket_str)
        test_lines = load_data(DEV_INTERMEDIATE_OUTPUT_LOC, period_str, delete=True)
        parsely_start_times = get_start_times(test_lines)
        print(parsely_start_times)
        keys = list(parsely_start_times.keys())
        total_num = 0
        for x in parsely_start_times.values():
            total_num+=x


        p_diff_max = (hm - keys[0]).seconds / 60
        p_diff_min = (hm - keys[-1]).seconds / 60
        max_instances = parsely_start_times[keys[0]]
        min_instances = parsely_start_times[keys[-1]]
        A = dict((datetime.datetime.strftime(key, "%Y-%m-%d %H:%M:%S"), value) for (key, value) in
                 parsely_start_times.items())

        A["maximum difference in timestamps:"] = "%.2f minutes" % p_diff_max

        dfk = {}
        dfk["max_delay_in_minutes"] = [p_diff_max]
        dfk["min_delay_in_minutes"] = [p_diff_min]
        dfk["num_records_with_max_delay"] = [max_instances]
        dfk["num_records_with_min_delay"] = [min_instances]
        dfk["total_number_of_records"] = [total_num]

        dfk["script_run_time"] = [bucket_str]
        dfk["bucket_str"] = [period_str]
        print(dfk)

        B = pd.DataFrame.from_dict(dfk)

        if is_redshift:
            boto_wrapper.s3_to_redshift(dataframe=B, table_name="su_buzzing_delay",
                                        engine=R.engine)

        if p_diff_max > THRESHOLD_MINUTES:
            return "ALERT: {delay} min delay in incoming data stream".format(delay=p_diff_max), A
        print("OK")
        return "OK", A

    except Exception as e:
        print(e.args)
        k = "EXCEPTION in TIMESTAMP ANALYSIS"
        message = {}
        message["message"] = k
        return "ALERT: Cannot retrieve data", message


def get_start_times(lines):
    start_times = defaultdict(int)
    for el in lines:
        st = el["startq"]
        st = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
        start_times[st] += 1
    sorted_keys = sorted(start_times)
    sorted_start_times = OrderedDict()
    for el in sorted_keys:
        sorted_start_times[el] = start_times[el]
    return sorted_start_times


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
