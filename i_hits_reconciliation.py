import pandas as pd
import json
from urllib import parse
import datetime
import re
from helpers.redshift_connector import RedshiftConnector
from common import load_data_new
import argparse
import os
from config import table_config, PROD_INTERMEDIATE_OUTPUT_LOC, DEV_INTERMEDIATE_OUTPUT_LOC, site_mappings, \
    HITS_TABLE_NAME, LIMIT, EPSILON, SITE_THRESHOLD, PERCENTAGE_THRESHOLD
from helpers.boto_connector import BotoConnector

boto_wrapper = BotoConnector(bucket="hearstdataservices", keypath="suguroglu/metrics/")

minutes_range = ["15", "30", "45", "60"]
buckets_range = [["05", "10", "15"], ["20", "25", "30"], ["35", "40", "45"], ["50", "55", "60"]]

R = RedshiftConnector()
dutc = datetime.datetime.utcnow() - datetime.timedelta(hours=2)  # process data from an hour ago
next_hour = dutc + datetime.timedelta(hours=1)

end_hour = next_hour.strftime("%Y-%m-%d %H:00:00")
begin_hour = dutc.strftime("%Y-%m-%d %H:00:00")

sql = "select clean_url,ic_site_id, count(distinct hash) as cv from {table_name} where hitdatetime_gmt >= \'{begin_hour}\' and hitdatetime_gmt < \'{end_hour}\' group by 1,2 having count(distinct hash) > 2 order by cv desc;".format(
    table_name=HITS_TABLE_NAME, end_hour=end_hour, begin_hour=begin_hour, limit=LIMIT)


def check_url(url):
    parsed = parse.urlparse(url)
    if parsed.path == "/":
        return False
    nl = str(parsed.netloc)
    path = str(parsed.path)
    if "m." in nl or "cdn." in nl or "video" in nl:
        return False
    if not re.findall("\d+", path):
        return False
    if is_art_check(url) == 'F':
        return False
    return True


def load_data_from_ihits():
    df = pd.read_sql(sql, R.engine)
    df["flag"] = df["clean_url"].apply(lambda x: check_url(x))
    df_a = df[df["flag"] == True]
    return df_a


def load_data_from_bucket(bucket_name, filename, is_download=True):
    if is_download:
        try:
            concat_a_day(bucket_name, filename)
        except Exception as e:
            raise Exception(e.args)
    pdf = pd.read_json(filename)
    pdfs = pdf[(pdf["startq"] >= begin_hour) & (pdf["startq"] < end_hour)]
    return pdfs


def number_of_sites(pdf):
    return pdf["cid"].nunique()


def is_art_check(url):
    parsed = parse.urlparse(url)
    full_path = parsed.path
    if re.compile('\d{5,}').search(full_path) != None:
        return 'T'
    return 'F'


def concat_a_day(bucket_name, filename):
    month = '{:02d}'.format(dutc.month)
    day = '{:02d}'.format(dutc.day)

    period_str = "{year}/{month}/{day}/{hour}".format(year=dutc.year, month=month,
                                                                         day=day, hour=dutc.hour)


    all_lines = load_data_new(bucket_name, period_str, delete=True)
    all_lines_json = [json.loads(el) for el in all_lines]
    if not all_lines:
        raise Exception("EMPTY BUCKET for period: {period}".format(period=period_str))
    f = open(filename, "w")
    f.write(json.dumps(all_lines_json))
    f.close()


def get_missing_urls(i_hits_df, pdf):
    i_hits_df = i_hits_df.head(LIMIT)
    parsely_missing = {}
    for idx, el in i_hits_df.iterrows():
        url = el["clean_url"]
        url = url.strip("/")
        url = url.replace('/amp', '/a')
        s = pdf[pdf["url"] == url]["pageviews"].sum()

        if s + EPSILON < el["cv"]:
            parsely_missing[url] = (s, el["cv"])
    return parsely_missing


def calculate_site_percentages(pdf, outname):
    A = pdf.groupby("cid")["pageviews"].sum().sort_values(ascending=False).reset_index()
    A = A.set_index("cid")
    A.to_csv(outname, sep="\t")


def get_missing_sites(i_hits_df, pdf):
    i_hits_pageviews = i_hits_df.groupby("ic_site_id")["cv"].sum().sort_values(ascending=False).reset_index()
    pdf_pageviews = pdf.groupby("cid")["pageviews"].sum().sort_values(ascending=False).reset_index()
    missing_sites = set(i_hits_pageviews["ic_site_id"]).difference(pdf["cid"])

    merged = pd.merge(i_hits_pageviews, pdf_pageviews, left_on="ic_site_id", right_on="cid", how="outer")
    # missing_from_parsely = merged[merged["cid"].isnull()].shape[0]
    common = merged[~merged["cid"].isnull() & ~merged["ic_site_id"].isnull()]
    common["diff"] = common["pageviews"] - common["cv"]
    common["diff_percentage"] = common["diff"] * 100 / common["cv"]
    common.header = ["ic_site_id_hits", "pageview_hits", "ic_site_id_buzzing", "pageview_buzzing", "diff",
                     "diff_percentage"]
    return missing_sites, common


def map_to_sites(ic_site_id):
    try:
        return site_mappings[ic_site_id]
    except:
        return ""


def main(is_icrossing=True, is_debug=False, is_download=True):
    month = '{:02d}'.format(dutc.month)
    day = '{:02d}'.format(dutc.day)

    parsely_filename = "{prefix}_{year}{month}{day}_{hour}.txt".format(prefix="parsely", year=dutc.year, month=month,
                                                                       day=day,
                                                                       hour=dutc.hour)
    try:
        pdf = load_data_from_bucket(DEV_INTERMEDIATE_OUTPUT_LOC, parsely_filename, is_download=is_download)
    except Exception as e:
        exception = {"message": e.args}
        print(exception)
        return "ALERT! intermediate buzzing output not OK", exception

    os.remove(parsely_filename)  # cleanup

    Apdf = pdf.groupby(["cid", "startq"])["pageviews"].sum().reset_index()
    Apdf["stream"] = "parsely"
    A = Apdf

    df = load_data_from_ihits()
    num_sites = number_of_sites(pdf)
    parsely_missing_urls = get_missing_urls(df, pdf)

    parsely_missing_sites, common_parsely = get_missing_sites(df, pdf)
    missing_url_percentage = len(list(parsely_missing_urls.keys())) * 100 / df.shape[0]

    different_sites = list(common_parsely[abs(common_parsely["diff_percentage"]) > PERCENTAGE_THRESHOLD]["ic_site_id"])

    high_percentage_sites = []
    for el in different_sites:
        high_percentage_sites.append(map_to_sites(el))

    stats = {}
    stats["Number_of_sites"] = num_sites
    stats["Missing_url_percentage"] = missing_url_percentage
    stats["Site_names_with_highest_difference_from_ihits"] = high_percentage_sites
    stats["bad_performing_sites"] = []
    stats["sites_missing_only_from_parsely"] = []

    if is_icrossing:
        icrossing_filename = "{prefix}_{year}{month}{day}_{hour}.txt".format(prefix="icrossing", year=dutc.year,
                                                                             month=month,
                                                                             day=day,
                                                                             hour=dutc.hour)

        idf = load_data_from_bucket(PROD_INTERMEDIATE_OUTPUT_LOC, icrossing_filename, is_download=is_download)
        os.remove(icrossing_filename)
        Bpdf = idf.groupby(["cid", "startq"])["pageviews"].sum().reset_index()
        Bpdf["stream"] = "icrossing"

        A = pd.concat([Apdf, Bpdf], axis=0)

        icrossing_missing_sites, common_icrossing = get_missing_sites(df, idf)
        all = pd.merge(common_icrossing, common_parsely, how="outer", on="ic_site_id")
        summary = all[(abs(all["diff_percentage_y"]) > 2 * abs(all["diff_percentage_x"]))]
        bad_performing_sites = [map_to_sites(x) for x in list(summary["ic_site_id"])]
        stats["bad_performing_sites"] = bad_performing_sites
        stats["sites_missing_only_from_parsely"] = list(parsely_missing_sites.difference(icrossing_missing_sites))
        stats["sitenames_missing_only_from_parsely"] = [map_to_sites(x) for x in
                                                        stats["sites_missing_only_from_parsely"]]

    if not is_debug:
        boto_wrapper.s3_to_redshift(dataframe=A, table_name=table_config["eval_intermediate"]["pdf_table"],
                                    engine=R.engine)
        boto_wrapper.s3_to_redshift(dataframe=common_parsely,
                                    table_name=table_config["eval_intermediate"]["i_hits_comparison"],
                                    engine=R.engine)


    if stats['Number_of_sites'] < SITE_THRESHOLD or len(stats["bad_performing_sites"]) > 10 or len(
            stats["sites_missing_only_from_parsely"]) > 10:
        print("ALERT! intermediate buzzing output not ok")
        return "ALERT! intermediate buzzing output not OK", stats

    print("OK")
    return "OK", stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--is_download', dest="is_download", type=bool, default=True)
    parser.add_argument('--is_icrossing', dest="is_icrossing", type=bool, default=True)
    parser.add_argument('--is_debug', dest="is_debug", type=bool, default=True)

    args = parser.parse_args()
    main(args.is_icrossing, args.is_debug, args.is_download)
