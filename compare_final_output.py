import argparse
import json, sys
import operator
import datetime
from print_evaluation_results import print_evaluation_results

UNIQUE_KEY = 'url_hash'
# UNIQUE_FIELD='url'
from collections import OrderedDict, defaultdict

DEFAULT_RANK = [100, 20]


class Evaluation(object):
    def __init__(self):
        self.number_baseline = -1
        self.number_test = -1
        self.sites = {}
        self.added_entries = defaultdict(list)
        self.common_entries = defaultdict(list)
        self.deleted_entries = defaultdict(list)
        self.added = defaultdict(int)
        self.deleted = defaultdict(int)
        self.common = defaultdict(int)
        self.percentages = defaultdict()
        self.unmatched_key_info = defaultdict(list)
        self.unmatched_keys = defaultdict(int)

        # self.top_unmatched_keys = None
        self.equal_number_of_keys = 0
        self.baseline_url_dict = OrderedDict()
        self.test_url_dict = OrderedDict()

    def set_article_numbers(self, number_baseline, number_test):
        self.number_baseline = number_baseline
        self.number_test = number_test


def compare_identical_articles(baseline_article, test_article, ev):
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
            # if isinstance(bv, (int,  float, complex)):
            ev.unmatched_key_info[el].append((el, bv, tv))
            # elif "-" in bv:
            #     try:
            #         bvt = datetime.datetime.strptime(bv, "%Y-%m-%d %H:%M:%S")
            #         tvt = datetime.datetime.strptime(tv, "%Y-%m-%d %H:%M:%S")
            #
            #         ev.unmatched_key_info[baseline_article[UNIQUE_KEY]].append(
            #             (el, baseline_article["url"], bv, tv, bvt-tvt))
            #     except:
            #         ev.unmatched_key_info[baseline_article[UNIQUE_KEY]].append(
            #             (el, baseline_article["url"], bv, tv,0))
            ev.unmatched_keys[el] += 1


def compare_articles(ev):
    for el, article in ev.baseline_url_dict.items():
        if el in ev.test_url_dict:
            test_article = ev.test_url_dict[el]
            compare_identical_articles(article, test_article, ev)
    ev.unmatched_keys = sorted(ev.unmatched_keys.items(), key=operator.itemgetter(1), reverse=True)


def check_duplicates(article_list):
    return len(list(set(article_list))) != len(article_list)


def get_url(url):
    try:
        return url.split(".com")[0] + '.com'
    except:
        print(url)


def calculate_percentages(ev):
    for k, v in ev.common.items():
        if k > -1:
            ev.percentages[k] = "%.2f" % (ev.common[k] * 100. / k)
        else:
            ev.percentages[k] = "%d" % (ev.common[k])


def calculate_set_stats(ev, N):
    if N > 0:
        s1 = set(list(ev.test_url_dict.keys())[0:N])
        s2 = set(list(ev.baseline_url_dict.keys())[0:N])
    else:
        s1 = set(list(ev.test_url_dict.keys()))
        s2 = set(list(ev.baseline_url_dict.keys()))
    ev.common[N] = len(s1.intersection(s2))
    ev.added[N] = len(s1.difference(s2))
    ev.deleted[N] = len(s2.difference(s1))


def analyze_by_site(ev):
    s1 = set(list(ev.test_url_dict.keys()))
    s2 = set(list(ev.baseline_url_dict.keys()))
    common = s1.intersection(s2)
    added = s1.difference(s2)
    deleted = s2.difference(s1)
    for v in common:
        cid = ev.test_url_dict[v]["siteid"]
        url = get_url(ev.test_url_dict[v]["url"])
        if cid not in ev.sites:

            ev.sites[cid] = (1, 0, 0, url)
        else:
            t1, t2, t3, t4 = ev.sites[cid]
            ev.sites[cid] = (t1 + 1, t2, t3, t4)
        ev.common_entries[cid].append(ev.test_url_dict[v]["url"])
    for v in added:
        cid = ev.test_url_dict[v]["siteid"]
        url = get_url(ev.test_url_dict[v]["url"])
        if cid not in ev.sites:

            ev.sites[cid] = (0, 1, 0, url)
        else:
            t1, t2, t3, t4 = ev.sites[cid]
            ev.sites[cid] = (t1, t2 + 1, t3, t4)
        ev.added_entries[cid].append(ev.test_url_dict[v]["url"])
    for v in deleted:
        cid = ev.baseline_url_dict[v]["siteid"]
        url = get_url(ev.baseline_url_dict[v]["url"])
        if cid not in ev.sites:
            ev.sites[cid] = (0, 0, 1, url)
        else:
            t1, t2, t3, t4 = ev.sites[cid]
            ev.sites[cid] = (t1, t2, t3 + 1, t4)

        ev.deleted_entries[cid].append(ev.baseline_url_dict[v]["url"])


def compare(baseline_json, test_json, narr):
    ev = Evaluation()
    ev.number_test = len(test_json)
    ev.number_baseline = len(baseline_json)

    for el in test_json:
        if el[UNIQUE_KEY] in ev.test_url_dict:
            print("DUPLICATE KEY! OVERWRITING")
        ev.test_url_dict[el[UNIQUE_KEY]] = el
    for el in baseline_json:
        if el[UNIQUE_KEY] in ev.baseline_url_dict:
            print("DUPLICATE KEY! OVERWRITING")
        ev.baseline_url_dict[el[UNIQUE_KEY]] = el

    ev.baseline_duplicates = check_duplicates(ev.baseline_url_dict)
    ev.test_duplicates = check_duplicates(ev.test_url_dict)
    for el in narr:
        if el == "max":
            calculate_set_stats(ev, N=max(ev.number_test, ev.number_baseline))
        else:
            calculate_set_stats(ev, N=el)

    calculate_percentages(ev)
    #calculate_site_metrics(ev)
    compare_articles(ev)
    # analyze_by_site(ev)
    return ev

def calculate_rank_metrics(ev):
    ev.baseline_rank = []
    for el,v in ev.baseline_url_dict.items():
        ev.baseline_rank.append([v["score"],v["siteid"],v["url"],v["bu"],v["trending"],v["trendingnow"],v["maxref"]])

    for el, v in ev.test_url_dict.items():
        ev.test_rank.append([v["score"],v["siteid"],v["url"],v["bu"],v["trending"],v["trendingnow"],v["maxref"]])


def main(test, baseline):
    global UNIQUE_KEY
    f = open(test)
    baseline_json = json.loads(f.read())
    f.close()
    f = open(baseline)
    test_json = json.loads(f.read())
    f.close()
    narr = [100, 20, "max"]
    if "res" in test:
        baseline_json = sorted(baseline_json, key=lambda k: (int(k['rank']), -float(k['score'])))
        test_json = sorted(test_json, key=lambda k: (int(k['rank']), -float(k['score'])))

    elif "ss" in test:
        baseline_json = sorted(baseline_json, key=lambda k: (int(k['cnt60'])))
        test_json = sorted(test_json, key=lambda k: (int(k['cnt60'])))
        UNIQUE_KEY = "sectionname"
        narr = [-1]

    elif "au" in test:
        baseline_json = sorted(baseline_json, key=lambda k: (int(k['visits60'])))
        test_json = sorted(test_json, key=lambda k: (int(k['visits60'])))
        UNIQUE_KEY = "author"
        narr = [-1]

    elif "bu" in test:
        UNIQUE_KEY = "bu"
        narr = [-1]

    return compare(baseline_json=baseline_json, test_json=test_json, narr=narr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--baseline', dest='baseline', type=str, required=True,
                        help='baseline file')
    parser.add_argument('--test_file', dest='test_file', type=str, required=True,
                        help='test file')

    args = parser.parse_args()
    main(args.test_file, args.baseline)
