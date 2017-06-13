import pandas as pd
import json
from urllib import parse
import datetime
import re
from helpers.redshift_connector import RedshiftConnector
from common import load_data
import argparse
import os

site_mappings = {10004: 'greenwichtime.com', 10005: 'dariennewsonline.com', 10006: 'fairfieldcitizenonline.com',
                 10008: 'newcanaannewsonline.com', 10009: 'newmilfordspectrum.com', 10011: 'westport-news.com',
                 10012: 'beaumontenterprise.com', 10013: 'mysanantonio.com', 10014: 'seattlepi.com', 10017: 'wcvb.com',
                 10018: 'wmur.com', 10019: 'mor-tv.com', 10020: 'wesh.com', 10021: 'cw18tv.com', 10022: 'wtae.com',
                 10023: 'wbaltv.com', 10024: 'wbal.com', 10025: '98online.com', 10026: 'wyff4.com', 10027: 'wpbf.com',
                 10028: 'wgal.com', 10029: 'wxii12.com', 10030: 'wlky.com', 10031: 'wmtw.com', 10032: 'mynbc5.com',
                 10033: 'kmbc.com', 10034: 'wisn.com', 10035: 'wlwt.com', 10036: 'wdsu.com', 10037: 'koco.com',
                 10038: 'kcci.com', 10039: 'ketv.com', 10040: 'wapt.com', 10041: '4029tv.com', 10042: 'kcra.com',
                 10043: 'my58.com', 10044: 'koat.com', 10045: 'kitv.com', 10046: 'ksbw.com', 10047: 'answerology.com',
                 10048: 'caranddriver.com', 10049: 'cosmopolitan.com', 10050: 'countryliving.com', 10051: 'elle.com',
                 10052: 'elledecor.com', 10054: 'esquire.com', 10055: 'goodhousekeeping.com',
                 10056: 'harpersbazaar.com', 10057: 'housebeautiful.com', 10058: 'marieclaire.com',
                 10060: 'popularmechanics.com', 10063: 'redbookmag.com', 10064: 'roadandtrack.com',
                 10065: 'seventeen.com', 10067: 'townandcountrymag.com', 10069: 'veranda.com', 10070: 'womansday.com',
                 10071: 'kaboodle.com', 1649: 'hearstmags.com', 1782: 'delish.com', 1845: 'houstonchronicle.com',
                 1887: 'sfchronicle.com', 1888: 'expressnews.com', 1915: 'allaboutsoap.co.uk', 1916: 'allaboutyou.com',
                 1917: 'bestdaily.co.uk', 1918: 'company.co.uk', 1919: 'cosmopolitan.co.uk', 1920: 'digitalspy.com',
                 1921: 'elleuk.com', 1922: 'elledecoration.co.uk', 1923: 'esquire.co.uk', 1924: 'fetcheveryone.com',
                 1925: 'goodhousekeeping.co.uk', 1926: 'handbag.com', 1927: 'harpersbazaar.co.uk',
                 1928: 'insidesoap.co.uk', 1929: 'marieclaire.co.uk', 1930: 'menshealth.co.uk', 1931: 'netdoctor.co.uk',
                 1932: 'realpeoplemag.co.uk', 1933: 'redonline.co.uk', 1935: 'reveal.co.uk', 1936: 'runnersworld.co.uk',
                 1937: 'sugarscape.com', 1938: 'triathletes-world-magazine.co.uk', 1939: 'womenshealthmag.co.uk',
                 1940: 'zest.co.uk', 1941: 'elle.it', 1942: 'elledecor.it', 1943: 'cosmopolitan.it',
                 1944: 'marieclaire.it', 1945: 'riders-online.it', 1946: 'cosmogirl.nl', 1947: 'cosmopolitan.nl',
                 1948: 'elle.nl', 1949: 'esquire.nl', 1950: 'menshealth.nl', 1951: 'fiscalert.nl', 1952: 'quotenet.nl',
                 1953: 'red.nl', 1954: 'ar-revista.com', 1955: 'casadiez.elle.es', 1956: 'crecerfeliz.es',
                 1957: 'elle.es/viajes', 1958: 'diezminutos.es', 1959: 'elle.es', 1960: 'elle.es/elledeco',
                 1961: 'emprendedores.es', 1962: 'fotogramas.es', 1963: 'micasarevista.com', 1964: 'nuevo-estilo.es',
                 1965: 'quemedices.diezminutos.es', 1966: 'quo.es', 1967: 'supertele.es',
                 1968: 'teleprograma.fotogramas.es', 1969: 'caranddriverthef1.com', 1984: 'womenshealthmag.nl',
                 1985: 'cosmopolitantv.es', 1992: 'peopleschoice.com', 1995: 'drozthegoodlife.com',
                 2008: 'the-wedding.jp', 2009: 'ellemaman.jp', 2010: 'kodomonokuni.info', 2011: 'michigansthumb.com',
                 2012: 'mrt.com', 2013: 'myplainview.com', 2014: 'ourmidland.com', 2015: 'theintelligencer.com',
                 2016: 'lmtonline.com', 2017: 'elle.com.tw', 2019: 'elleeten.nl', 2020: 'elle.co.jp',
                 2021: 'ellegirl.jp', 2022: 'elleshop.jp', 2023: 'hearst.co.jp', 2024: '25ans.jp', 2025: 'mensclub.jp',
                 2026: 'trip.kyoto.jp', 2027: 'harpersbazaar.jp', 2028: 'cosmobody.com', 2030: 'mcchina.com',
                 2031: 'psychologies.com.cn', 2032: 'elleshop.com.cn', 2034: 'cad.com.cn', 2035: 'femina.com.cn',
                 2036: 'hearst.com.cn', 2059: 'countryliving.co.uk', 2060: 'housebeautiful.co.uk', 2063: 'ctnews.com',
                 2065: 'hearstdigital.com', 2068: 'wvtm13.com', 2069: 'cosmopolitan.ng', 2071: 'cosmopolitan.dk',
                 2072: 'cosmopolitan.no', 2077: 'cosmopolitan.se', 2078: 'wjcl.com', 2081: 'harpersbazaar.nl',
                 2084: 'southeasttexas.com', 2087: 'motor.com', 2088: '7yearsyounger.com', 2089: 'bestproducts.com',
                 2090: 'lennyletter.com', 2094: 'wearesweet.co', 2095: 'prima.co.uk', 2096: 'cosmopolitan.in',
                 2097: 'cosmopolitan-jp.com', 2098: 'cosmopolitan.com.tw', 2100: 'gioia.it',
                 2101: 'harpersbazaar.com.tw', 2104: 'womansdayspain.es', 2105: 'thehour.com', 2108: 'hearst.com',
                 2109: 'harpersbazaar.es', 2111: 'sellittexas.com', 2112: 'yourconroenews.com',
                 2114: 'townandcountrymag.co.uk', 2115: 'khtvnews.com', 4: 'sfgate.com', 5: 'stamfordadvocate.com',
                 6: 'ctpost.com', 7: 'chron.com', 8: 'newstimes.com', 9: 'timesunion.com'}

minutes_range = ["15", "30", "45", "60"]
buckets_range = [["05", "10", "15"], ["20", "25", "30"], ["35", "40", "45"], ["50", "55", "60"]]

TABLE_NAME = "i_hits"
LIMIT = 1000
EPSILON = 40
PARSELY_BUCKET = "s3://hearstdataservices/buzzing/parselyjson/"
BASELINE_BUCKET = 's3://hearstkinesisdata/processedsparkjsonix/'
SITE_THRESHOLD = 50
PERCENTAGE_THRESHOLD = 30
I_CROSSING = True
R = RedshiftConnector()
dutc = datetime.datetime.utcnow()
next_hour = dutc + datetime.timedelta(hours=1)

end_hour = next_hour.strftime("%Y-%m-%d %H:00:00")
begin_hour = dutc.strftime("%Y-%m-%d %H:00:00")
sql = "select clean_url,ic_site_id, count(distinct hash) as cv from {table_name} where hitdatetime_gmt >= \'{begin_hour}\' and hitdatetime_gmt < \'{end_hour}\' group by 1,2 having count(distinct hash) > 2 order by cv desc;".format(
    table_name=TABLE_NAME, end_hour=end_hour, begin_hour=begin_hour, limit=LIMIT)


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
        concat_a_day(bucket_name, filename)
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
    all_lines = []
    for h in range(dutc.hour - 1, dutc.hour + 1):
        print(h)
        for i, min in enumerate(minutes_range):
            for buc in buckets_range[i]:
                hour = '{:02d}'.format(h)
                period_str = "{year}/{month}/{day}/{hour}/{minute}/{bucket}/".format(year=dutc.year, month=month,
                                                                                     day=day, hour=hour, minute=min,
                                                                                     bucket=buc)
                test_lines = load_data(bucket_name, period_str, delete=True)

                all_lines += test_lines

    f = open(filename, "w")

    f.write(json.dumps(all_lines))
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

    return missing_sites, common


def map_to_sites(ic_site_id):
    try:
        return site_mappings[ic_site_id]
    except:
        return ""


def alert(output_folder="out/", is_download=False):
    month = '{:02d}'.format(dutc.month)
    day = '{:02d}'.format(dutc.day)
    parsely_filename = "{prefix}_{year}{month}{day}_{hour}.txt".format(prefix="parsely", year=dutc.year, month=month,
                                                                       day=day,
                                                                       hour=dutc.hour)
    icrossing_filename = "{prefix}_{year}{month}{day}_{hour}.txt".format(prefix="icrossing", year=dutc.year,
                                                                         month=month,
                                                                         day=day,
                                                                         hour=dutc.hour)
    # parsely_filename ="/Users/suguroglu/Dev/buzzing_evaluation/parsely_2017612_20.txt"
    # icrossing_filename = "/Users/suguroglu/Dev/buzzing_evaluation/icrossing_2017612_20.txt"
    pdf = load_data_from_bucket(PARSELY_BUCKET, parsely_filename, is_download=is_download)
    idf = load_data_from_bucket(BASELINE_BUCKET, icrossing_filename, is_download=is_download)
    df = load_data_from_ihits()
    num_sites = number_of_sites(pdf)
    parsely_missing_urls = get_missing_urls(df, pdf)
    parsely_missing_sites, common_parsely = get_missing_sites(df, pdf)
    missing_url_percentage = len(list(parsely_missing_urls.keys())) * 100 / df.shape[0]
    site_percentage_out_file = os.path.join(output_folder, "site_counts_{begin_hour}.txt".format(begin_hour=begin_hour))
    i_hits_comparison_file = os.path.join(output_folder, "i_hits_{begin_hour}.txt".format(begin_hour=begin_hour))

    calculate_site_percentages(pdf, site_percentage_out_file)
    stats = {}
    stats["Number_of_sites"] = num_sites
    stats["Missing_url_percentage"] = missing_url_percentage
    different_sites = list(common_parsely[abs(common_parsely["diff_percentage"]) > PERCENTAGE_THRESHOLD]["ic_site_id"])
    common_parsely.to_csv(i_hits_comparison_file, sep="\t")
    high_percentage_sites = []
    for el in different_sites:
        high_percentage_sites.append(map_to_sites(el))
    stats["Site_names_with_highest_difference_from_ihits"] = high_percentage_sites

    if I_CROSSING:
        icrossing_missing_sites, common_icrossing = get_missing_sites(df, idf)
        all = pd.merge(common_icrossing, common_parsely, how="outer", on="ic_site_id")
        summary = all[(abs(all["diff_percentage_y"]) > 1.5 * abs(all["diff_percentage_x"]))]
        badly_performing_site_out_file = os.path.join(output_folder,
                                                      "bad_performing_{begin_hour}.txt".format(begin_hour=begin_hour))

        summary.to_csv(badly_performing_site_out_file, sep="\t")
        bad_performing_sites = [map_to_sites(x) for x in list(summary["ic_site_id"])]
        stats["bad_performing_sites"] = bad_performing_sites
        stats["sites_missing_only_from_parsely"] = list(parsely_missing_sites.difference(icrossing_missing_sites))
        stats["sitenames_missing_only_from_parsely"] = [map_to_sites(x) for x in
                                                        stats["sites_missing_only_from_parsely"]]
    stats_out_file = os.path.join(output_folder, "stats_{begin_hour}.txt".format(begin_hour=begin_hour))

    f = open(stats_out_file, "w")
    for k, v in stats.items():
        k = k.replace("_", " ")
        k = k.title()
        if type(v) == list:
            vstr = ",".join([str(x) for x in v])
        else:
            vstr = str(v)
        f.write(k + "\t" + vstr + "\n")
    f.close()
    if stats['Number_of_sites'] < 50 or len(stats["bad_performing_sites"]) > 10 or len(
            [stats["sites_missing_only_from_parsely"]]) > 10:
        print("ALERT!")
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--out', dest='out', type=str, default="out/")
    parser.add_argument('--is_download', dest="is_download", type=bool, default=False)

    args = parser.parse_args()
    alert(args.out, args.is_download)
