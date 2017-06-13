import os
import datetime
dt = datetime.datetime.utcnow()
dt = datetime.datetime.strftime(dt, "%Y_%m_%d")


def _print_top_unmatched_keys(ev):
    for key, val in ev.unmatched_keys:
        pval = "%.2f" % (val * 100. / ev.number_baseline)
        _print_message(key, pval)


def _print_article_stats(ev):
    _print_message("Number of articles in baseline", (ev.number_baseline))
    _print_message("Number of articles in test", (ev.number_test))

    if ev.number_baseline != ev.number_test:
        _print_message("ERROR", "Number of articles does not match")

    for k, v in ev.common.items():
        if k > 1000:
            print("OVERALL STATS:")
        else:
            print("STATS FOR " + str(k).upper())
        _print_message("Number of common items", ev.common[k])
        _print_message("Number of deleted items", ev.deleted[k])
        _print_message("Number of added items", ev.added[k])


# url, key, baseline , new
def _print_unmatched_detailed_info(ev):
    fname = "unmatched_detailed_%s.tsv" %(str(dt))
    f = open(fname, "w")
    for k, varr in ev.unmatched_key_info.items():
        for v in varr:
            try:
                if type(k)==str:
                    f.write("\t".join([k] + [str(el) for el in v]))
                else:
                    k = "\t".join(k)
                    f.write("\t".join([k] + [str(el) for el in v]))
                f.write("\n")
            #    print("\t".join([k] + [str(el) for el in v]))
            except:
                continue
    f.close()

def _print_overall_percentage(ev):
    for k, v in ev.common.items():
        if k > 1000:
            print("OVERALL STATS:")
        else:
            print("STATS FOR " + str(k).upper())
        if k > -1:
            _print_message("Percentage common", "%.2f" % (ev.common[k] * 100 / float(k)))
            _print_message("Percentage deleted", "%.2f" % (ev.deleted[k] * 100 / float(k)))
            _print_message("Percentage added", "%.2f" % (ev.added[k] * 100 / float(k)))
        else:
            _print_message("Common items", "%d" % (ev.common[k]))
            _print_message("Deleted items", "%d" % (ev.deleted[k]))
            _print_message("Added items", "%d" % (ev.added[k]))


def _print_message(message, val):
    print("%s\t%s" % (message.upper(), str(val)))


def _print_site(ev):
    for k, v in ev.sites.items():
        print("\t".join([str(k)] + [str(x) for x in v]))

def _print_entries(ev):
    for el, v in ev.common_entries.items():
        for k in v:
            print(str(el) + "\t" + str(k)  + "\tCOMMON")

    for el, v in ev.deleted_entries.items():
        for k in v:
            print(str(el)+"\t" + str(k) +"\tDELETED")

    for el, v in ev.added_entries.items():
        for k in v:
            print(str(el)+"\t" + str(k)  + "\tADDED")

def print_evaluation_results(ev):
    print("ARTICLE STATS")
    _print_overall_percentage(ev)
    print("******")
    print("TOP UNMATCHED FIELDS")
    _print_top_unmatched_keys(ev)
    print("SITE_STATS")
    _print_site(ev)
    print("SPECIFIC")
    _print_entries(ev)
    print("*******"
          "*")
    _print_unmatched_detailed_info(ev)


def print_graphite_command(ev):
    for k, v in ev.common.items():
        pval = "%.2f" % (ev.common[k] * 100 / float(k))

        if k > 1000:

            cmd = "local.buzzing.parity_overall"
        else:
            cmd = "local.buzzing.parity_" + str(k)
        final = "echo \"" + cmd + " " + pval + " `date +%s`\" | nc -c  127.0.0.1 2003"
        print(final)
        os.system(final)
