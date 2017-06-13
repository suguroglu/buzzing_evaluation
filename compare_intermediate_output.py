import argparse
import json
import operator
from compare_final_output import Evaluation, compare_articles, calculate_set_stats, get_url

UNIQUE_KEY = 'url_hash'
import datetime

from collections import defaultdict

dt = datetime.datetime.utcnow()
dt = datetime.datetime.strftime(dt, "%Y_%m_%d")


def intermediate_compare_identical_articles(ky, baseline_article, test_article, ev):
    baseline_keys = list(baseline_article.keys())
    test_keys = list(test_article.keys())
    eq_number_of_keys = len(baseline_keys) == len(test_keys)
    ev.equal_number_of_keys += int(eq_number_of_keys)

    for el in baseline_keys:
        bv = baseline_article[el]
        if not el in test_article:
            continue

        tv = test_article[el]
        if bv != tv:
            ev.unmatched_key_info[ky].append((el, bv, tv))
            ev.unmatched_keys[el] += 1


def intermediate_compare(test_json, baseline_json):
    ev = Evaluation()
    ev.number_test = len(test_json)
    ev.number_baseline = len(baseline_json)
    parsely_start_times = defaultdict(int)
    baseline_start_times = defaultdict(int)
    for el in test_json:
        mkey = (el["startq"], el["url"], el["cid"], el["cdid"])
        parsely_start_times[el["startq"]] += 1
        ev.test_url_dict[mkey] = el
    for el in baseline_json:
        mkey = (el["startq"], el["url"], el["cid"], el["cdid"])
        ev.baseline_url_dict[mkey] = el
        baseline_start_times[el["startq"]] += 1

    parsely_start_times = sorted(parsely_start_times)
    baseline_start_times = sorted(baseline_start_times)

    print(baseline_start_times)
    print(parsely_start_times)

    calculate_set_stats(ev, N=-1)
    intermediate_compare_articles(ev)
    return ev


def analyze_by_site(ev):
    s1 = set(list(ev.test_url_dict.keys()))
    s2 = set(list(ev.baseline_url_dict.keys()))
    common = s1.intersection(s2)
    added = s1.difference(s2)
    deleted = s2.difference(s1)
    for v in common:
        cid = ev.test_url_dict[v]["cid"]
        url = get_url(ev.test_url_dict[v]["url"])
        if cid not in ev.sites:

            ev.sites[cid] = (1, 0, 0, url)
        else:
            t1, t2, t3, t4 = ev.sites[cid]
            ev.sites[cid] = (t1 + 1, t2, t3, t4)
        ev.common_entries[cid].append(ev.test_url_dict[v]["url"])
    for v in added:
        cid = ev.test_url_dict[v]["cid"]
        url = get_url(ev.test_url_dict[v]["url"])
        if cid not in ev.sites:

            ev.sites[cid] = (0, 1, 0, url)
        else:
            t1, t2, t3, t4 = ev.sites[cid]
            ev.sites[cid] = (t1, t2 + 1, t3, t4)
        ev.added_entries[cid].append(ev.test_url_dict[v]["url"])
    for v in deleted:
        cid = ev.baseline_url_dict[v]["cid"]
        url = get_url(ev.baseline_url_dict[v]["url"])
        if cid not in ev.sites:
            ev.sites[cid] = (0, 0, 1, url)
        else:
            t1, t2, t3, t4 = ev.sites[cid]
            ev.sites[cid] = (t1, t2, t3 + 1, t4)

        ev.deleted_entries[cid].append(ev.baseline_url_dict[v]["url"])


def intermediate_compare_articles(ev):
    for el, article in ev.baseline_url_dict.items():
        if el in ev.test_url_dict:
            test_article = ev.test_url_dict[el]
            intermediate_compare_identical_articles(el, article, test_article, ev)
    ev.unmatched_keys = sorted(ev.unmatched_keys.items(), key=operator.itemgetter(1), reverse=True)


def main(test, baseline):
    f = open(test)
    test_lines = [json.loads(x) for x in f.readlines()]
    f.close()
    f = open(baseline)
    baseline_lines = [json.loads(x) for x in f.readlines()]
    f.close()
    print("Baseline Count: %d" % len(baseline_lines))
    print("Parsely Count: %d" % len(test_lines))

    return intermediate_compare(test_lines, baseline_lines)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--baseline', dest='baseline', type=str, required=True,
                        help='baseline file')
    parser.add_argument('--test_file', dest='test_file', type=str, required=True,
                        help='test file')

    args = parser.parse_args()
    main(args.test_file, args.baseline)
