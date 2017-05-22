import argparse
import json
import operator
from print_evaluation_results import print_evaluation_results
UNIQUE_KEY = 'url_hash'
from collections import OrderedDict, defaultdict

DEFAULT_RANK = [100, 20]


class Evaluation(object):
    def __init__(self):
        self.number_baseline = -1
        self.number_test = -1
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


def compare_identical_articles(baseline_article,test_article,ev):
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
            ev.unmatched_key_info[baseline_article['url']].append((el,bv,tv))
            # print("DIFFERENT CONTENTS FOR ARTICLE %s TEST %s:\tBASELINE:\t%s\tTEST\t%s" % (
            # baseline_article['url_hash'], el, bv, tv))
            ev.unmatched_keys[el] += 1


def compare_articles(ev):

    for el, article in ev.baseline_url_dict.items():
        if el in ev.test_url_dict:
            test_article = ev.test_url_dict[el]
            compare_identical_articles(article, test_article, ev)
    ev.unmatched_keys = sorted(ev.unmatched_keys.items(), key=operator.itemgetter(1), reverse=True)



def check_duplicates(article_list):
    return len(list(set(article_list))) != len(article_list)




def calculate_set_stats(ev, N):
    s1 = set(list(ev.test_url_dict.keys())[0:N])
    s2 = set(list(ev.baseline_url_dict.keys())[0:N])
    ev.common[N] = len(s1.intersection(s2))
    ev.added[N] = len(s1.difference(s2))
    ev.deleted[N] = len(s2.difference(s1))


def compare(baseline_json, test_json, is_print=True):
    ev = Evaluation()
    ev.number_test = len(test_json)
    ev.number_baseline = len(baseline_json)

    for el in test_json:
        ev.test_url_dict[el[UNIQUE_KEY]] = el
    for el in baseline_json:
        ev.baseline_url_dict[el[UNIQUE_KEY]] = el

    ev.baseline_duplicates = check_duplicates(ev.baseline_url_dict)
    ev.test_duplicates = check_duplicates(ev.test_url_dict)

    calculate_set_stats(ev, N=max(ev.number_test, ev.number_baseline))
    calculate_set_stats(ev, N=100)
    calculate_set_stats(ev, N=20)
    calculate_percentages(ev)

    compare_articles(ev)

    if is_print:
        print_evaluation_results(ev)
    return ev


def calculate_percentages(ev):
    for k,v in ev.common.items():
        ev.percentages[k] = "%.2f" %(ev.common[k]*100./k)

def main(test, baseline):
    f = open(test)
    baseline_json = json.loads(f.read())
    f.close()
    f = open(baseline)
    test_json = json.loads(f.read())
    f.close()

    baseline_json = sorted(baseline_json, key=lambda k: (int(k['rank']), -float(k['score'])))
    test_json = sorted(test_json, key=lambda k: (int(k['rank']), -float(k['score'])))

    return compare(test_json, baseline_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--baseline', dest='baseline', type=str, required=True,
                        help='baseline file')
    parser.add_argument('--test_file', dest='test_file', type=str, required=True,
                        help='test file')

    args = parser.parse_args()
    main(args.test_file, args.baseline)
