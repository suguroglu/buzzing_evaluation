import datetime
import json
import os
import pandas as pd
from helpers.boto_connector import BotoConnector
from helpers.redshift_connector import RedshiftConnector
from config import table_config, FINAL_OUTPUT_LOG_FOLDER

THRESHOLD = 70
COMMON_KEY = "url_hash"

dutc = datetime.datetime.utcnow()
begin_hour = dutc.strftime("%Y-%m-%d %H:%M:%S")
timestamp = dutc.strftime("%Y-%m-%d %H:%M")

boto_wrapper = BotoConnector(bucket="hearstdataservices", keypath="suguroglu/metrics/")
redshift_connector = RedshiftConnector()


def analyze(bdf, pdf, N=None, rank=1):
    gr1 = bdf.sort_values(by="score", ascending=False)
    gr2 = pdf.sort_values(by="score", ascending=False)
    if N:
        gr1 = gr1.head(N)
        gr2 = gr2.head(N)

    s1 = set(gr1[COMMON_KEY])
    s2 = set(gr2[COMMON_KEY])
    common = s1.intersection(s2)
    percentage_common = len(common) * 100 / len(s1)
    merged = gr1.merge(gr2, on="url")

    stats = {}
    header = bdf.columns
    for el in header:

        if el == "url":
            continue

        el_x = el + "_x"
        el_y = el + "_y"
        if merged[el_x].dtype == object:
            diff = (merged[el_x] == merged[el_y]).sum() * 100 / merged[el_x].shape[0]
        elif len(merged[el_x].unique()) == 2:
            diff = (merged[el_x] == merged[el_y]).sum() * 100 / merged[el_x].shape[0]
        else:
            a = (merged[el_x] - merged[el_y]) / merged[el_x]
            diff = a.mean()

        stats[el] = diff
    stats = pd.DataFrame(stats, index=[0])
    stats["percentage_common"] = percentage_common
    stats["rank"] = rank
    stats["size"] = gr1.shape[0]
    return stats


def compare(test_file_loc, baseline_file_loc, period, is_debug):
    print(begin_hour)
    if period == "res5":
        suffix = ""
    else:
        suffix = "_" + period

    bdf = pd.read_json(baseline_file_loc)
    pdf = pd.read_json(test_file_loc)
    try:
        timestamps = list(pdf["dslastupdated"].unique())
        if len(timestamps) < 1:
            return False, "NO DATA IN HERE"

        dslast = datetime.datetime.utcfromtimestamp(int(timestamps[0]))
        print(dslast)
        print(dutc.hour)
        if dslast.day < dutc.day or dslast.hour < dutc.hour:
            print("ALERT dslastupdated old")
            return True, "DSLASTUPDATED IS OLD"
    except:
        return True, "UNKNOWN EXCEPTION"

    pdf["stream"] = "parsely"
    bdf["stream"] = "icrossing"
    A = pd.concat([bdf, pdf], axis=0)
    A["timestamp"] = timestamp
    A["sitename"] = A["sitename"].astype(str)

    bsv = bdf["siteid"].value_counts().keys()
    asv = pdf.siteid.value_counts().keys()

    top_ten_sites = len(set(bsv[0:10]).intersection(set(asv[0:10]))) * 100 / 10
    stats = analyze(bdf, pdf)
    stats["time"] = timestamp
    stats_20 = analyze(bdf, pdf, N=20)

    stats_100 = analyze(bdf, pdf, N=100)
    stats_100["time"] = timestamp

    results = {}
    results["Top 10 sites Percentage"] = "%.2f" % top_ten_sites
    results["Rank 1 URL Aggrement Percentage"] = "%.2f" % stats["percentage_common"].iat[0]
    results["Top 20 URLs in Rank 1 Agreement Percentage"] = "%.2f" % stats_20["percentage_common"].iat[0]
    results["Top 100 URLs in Rank 1 Agreement Percentage"] = "%.2f" % stats_100["percentage_common"].iat[0]


    if not os.path.exists(FINAL_OUTPUT_LOG_FOLDER):
        os.makedirs(FINAL_OUTPUT_LOG_FOLDER)

    print("Top 20 aggrement {res20}".format(res20=results["Top 20 URLs in Rank 1 Agreement Percentage"]))
    print("Top 100 aggrement {res20}".format(res20=results["Top 100 URLs in Rank 1 Agreement Percentage"]))
    alert = False

    if float(results["Top 100 URLs in Rank 1 Agreement Percentage"]) < THRESHOLD or float(
            results["Top 20 URLs in Rank 1 Agreement Percentage"]) < THRESHOLD:
        alert = True

    if not is_debug:
        print(
            "Writing to redshift: {table1}, {table2}".format(table1=table_config["eval_final"]["stats_table"] + suffix,
                                                             table2=table_config["eval_final"]["pdf_table"] + suffix))


        #su_buzzing_stats_comparison
        boto_wrapper.s3_to_redshift(dataframe=stats, table_name=table_config["eval_final"]["stats_table"] + suffix,
                                    engine=redshift_connector.engine)

        boto_wrapper.s3_to_redshift(dataframe=stats_100, table_name=table_config["eval_final"]["stats_table_100"] + suffix,
                                    engine=redshift_connector.engine)

        boto_wrapper.s3_to_redshift(dataframe=A, table_name=table_config["eval_final"]["pdf_table"] + suffix,
                                    engine=redshift_connector.engine)

    return alert, results
