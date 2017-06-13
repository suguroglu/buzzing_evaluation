import argparse
import pandas as pd
import datetime
import json
threshold = 70
common_key = "url_hash"
import os
dutc = datetime.datetime.utcnow()
begin_hour = dutc.strftime("%Y-%m-%d %H:%M:%S")
def analyze(bdf, pdf, N=None,rank=1):
    
    gr1 = bdf[bdf["rank"] == rank]
    gr2 = pdf[pdf["rank"] == rank]
    gr1 = gr1.sort_values(by="score",ascending=False)
    gr2 = gr2.sort_values(by="score",ascending=False)
    if N:
        gr1 = gr1.head(N)
        gr2 = gr2.head(N)


    s1 = set(gr1[common_key])
    s2 = set(gr2[common_key])
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

def main(test_file_loc, baseline_file_loc, output_folder="final_out/"):
    bdf = pd.read_json(baseline_file_loc)
    pdf = pd.read_json(test_file_loc)
    bsv = bdf["siteid"].value_counts().keys()
    asv = pdf.siteid.value_counts().keys()


    top_ten_sites = len(set(bsv[0:10]).intersection(set(asv[0:10]))) * 100 / 10
    stats = analyze(bdf,pdf)
    stats_20 = analyze(bdf,pdf,N=20)
    stats_100 = analyze(bdf, pdf,N=100)

    results = {}
    results["top_10_site_aggrement"] = top_ten_sites
    results["all_rank_1_aggrement"] = stats["percentage_common"].iat[0]
    results["top_20_aggreement_in_rank_1"] = stats_20["percentage_common"].iat[0]
    results["top_100_aggreement_in_rank_1"] = stats_100["percentage_common"].iat[0]
    json_str = json.dumps(results)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    stats_out_file = os.path.join(output_folder, "stats_{begin_hour}.txt".format(begin_hour=begin_hour))
    f = open(stats_out_file,"w")
    f.write(json_str)
    f.close()
    print("Top 20 aggrement {res20}".format(res20=results["top_20_aggreement_in_rank_1"]))
    print("Top 100 aggrement {res20}".format(res20=results["top_100_aggreement_in_rank_1"]))
    alert= False
    if float(results['all_rank_1_aggrement']) < 70 or results["top_20_aggreement_in_rank_1"] < 70:
        alert = True
    return alert, stats_out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--baseline', dest='baseline', type=str, required=True,
                        help='baseline file')
    parser.add_argument('--test_file', dest='test_file', type=str, required=True,
                        help='test file')

    args = parser.parse_args()
    main(args.test_file, args.baseline)
