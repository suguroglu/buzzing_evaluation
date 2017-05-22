import os
def _print_top_unmatched_keys(ev):

    for key,val in ev.unmatched_keys:
        pval = "%.2f" %(val*100./ev.number_baseline)
        _print_message(key, pval)

def _print_article_stats(ev):
    _print_message("Number of articles in baseline", (ev.number_baseline))
    _print_message("Number of articles in test", (ev.number_test))

    if ev.number_baseline != ev.number_test:
        _print_message("ERROR", "Number of articles does not match")

    for k, v in ev.common.items():
        if k > 1000:
            print ("OVERALL STATS:")
        else:
            print("STATS FOR " + str(k).upper())
        _print_message("Number of common items", ev.common[k])
        _print_message("Number of deleted items", ev.deleted[k])
        _print_message("Number of added items", ev.added[k])

def _print_unmatched_detailed_info(ev):
    for k,varr in ev.unmatched_key_info.items():
        for v in varr:
            print(k+"\t"+v[0]+"\t"+str(v[1])+"\t"+str(v[2]))

def _print_overall_percentage(ev):
    for k, v in ev.common.items():
        if k > 1000:
            print("OVERALL STATS:")
        else:
            print("STATS FOR " + str(k).upper())
        _print_message("Percentage common", "%.2f" %(ev.common[k]*100 / float(k)))
        _print_message("Percentage deleted", "%.2f" %(ev.deleted[k]*100 / float(k)))
        _print_message("Percentage added", "%.2f" %(ev.added[k] *100 / float(k)))


def _print_message(message, val):
    print(message.upper() + ":\t" + str(val))

def print_evaluation_results(ev):
    print("ARTICLE STATS")
    _print_overall_percentage(ev)
    print("******")
    print("TOP UNMATCHED FIELDS")
    _print_top_unmatched_keys(ev)
    print("********")
    _print_unmatched_detailed_info(ev)

def print_graphite_command(ev):
    for k, v in ev.common.items():
        pval = "%.2f" % (ev.common[k] * 100 / float(k))

        if k > 1000:

            cmd = "local.buzzing.parity_overall"
        else:
            cmd = "local.buzzing.parity_"+str(k)
        final = "echo \""+cmd+" "+ pval+" `date +%s`\" | nc -c  127.0.0.1 2003"
        print(final)
        os.system(final)