import argparse
import datetime
import json
import os

import pandas as pd
from helpers.boto_connector import BotoConnector
from helpers.redshift_connector import RedshiftConnector

from common import load_data_new
from config import PROD_INTERMEDIATE_OUTPUT_LOC, DEV_INTERMEDIATE_OUTPUT_LOC, site_mappings

boto_wrapper = BotoConnector(bucket="hearstdataservices", keypath="suguroglu/metrics/")

R = RedshiftConnector()
script_run_time = datetime.datetime.utcnow()
dutc = datetime.datetime.utcnow() - datetime.timedelta(hours=2)  # process data from two hours ago


def load_data_from_bucket(bucket_name, filename):

    try:
        concat_a_day(bucket_name, filename)
    except Exception as e:
        try:
            concat_a_day(bucket_name, filename)
        except:
            raise Exception(e.args)
    pdf = pd.read_json(filename)
    return pdf



def concat_a_day(bucket_name, filename):
    month = '{:02d}'.format(dutc.month)
    day = '{:02d}'.format(dutc.day)
    hour = '{:02d}'.format(dutc.hour)
    period_str = "{year}/{month}/{day}/{hour}".format(year=dutc.year, month=month,
                                                                         day=day, hour=hour)

    print(period_str)
    all_lines = load_data_new(bucket_name, period_str, delete=True)
    all_lines_json = [json.loads(el) for el in all_lines]
    if not all_lines:
        raise Exception("EMPTY BUCKET for period: {period}".format(period=period_str))
    f = open(filename, "w")
    f.write(json.dumps(all_lines_json))
    f.close()



def map_to_sites(ic_site_id):
    try:
        return site_mappings[ic_site_id]
    except:
        return ""


def main(is_icrossing=True, is_debug=False):
    month = '{:02d}'.format(dutc.month)
    day = '{:02d}'.format(dutc.day)
    hour = '{:02d}'.format(dutc.hour)


    try:

        parsely_filename = "{prefix}_{year}{month}{day}_{hour}.txt".format(prefix="parsely", year=dutc.year,
                                                                           month=month,
                                                                           day=day,
                                                                           hour=hour)

        pdf = load_data_from_bucket(DEV_INTERMEDIATE_OUTPUT_LOC, parsely_filename)
        pdf["stream"] = "parsely"
        pdf["script_runtime"] = script_run_time.strftime("%Y-%m-%d %H:00:00")
        B = pdf
        os.remove(parsely_filename)  # cleanup

    except Exception as e:
        exception = {"message": e.args}
        print(exception)
        return "ALERT! intermediate buzzing output not OK", exception


    if is_icrossing:
        print("Running ICrossing")

        try:
            icrossing_filename = "{prefix}_{year}{month}{day}_{hour}.txt".format(prefix="icrossing", year=dutc.year,
                                                                                 month=month,
                                                                                 day=day,
                                                                                 hour=hour)

            idf = load_data_from_bucket(PROD_INTERMEDIATE_OUTPUT_LOC, icrossing_filename)
            idf["stream"] = "icrossing"
            idf["script_runtime"] = script_run_time.strftime("%Y-%m-%d %H:00:00")
            B = pd.concat([pdf, idf], axis=0)

            os.remove(icrossing_filename)

        except Exception as e:
            exception = {"message": e.args}
            print(exception)
            return "ALERT! intermediate buzzing output not OK", exception

    B = B[["cid","cdid","url","pageviews","startq","url_hash","stream","script_runtime"]]
    if not is_debug:
        boto_wrapper.s3_to_redshift(dataframe=B, table_name="su_raw_intermediate_buzzing",
                                    engine=R.engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--is_icrossing', dest="is_icrossing", type=bool, default=True)
    parser.add_argument('--is_debug', dest="is_debug", type=bool, default=True)

    args = parser.parse_args()
    main(args.is_icrossing, args.is_debug)
