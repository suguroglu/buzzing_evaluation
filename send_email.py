import smtplib
import json
from jinja2 import Environment, FileSystemLoader
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ts_analysis import main as ts_analysis_main
from eval_final import main as eval_final_main
from i_hits_reconciliation import main as i_hits_main
import argparse
from_email = "suguroglu@hearst.com"
#to_email = "hds-notifications@hearst.com"
to_email = "suguroglu@hearst.com"

env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')))
template = env.get_template('email_template.html')



def send_email(title_str, msg_dict, is_debug):
    msg = MIMEMultipart('alternative')

    if msg_dict:

        rendered_email = template.render(results=msg_dict, title=title_str)
        part2 = MIMEText(rendered_email, 'html')
        msg.attach(part2)

    msg['Subject'] = title_str
    msg['From'] = from_email
    msg['To'] = to_email
    s = smtplib.SMTP("localhost")
    if not is_debug:
       s.sendmail(from_email, [to_email], msg.as_string())
    s.quit()

def run_delay_report(is_debug):
    delay_alert, p_diff_max = ts_analysis_main()
    if "OK" not in delay_alert:
        send_email("Buzzing Warning: Delay in buzzing kinesis", p_diff_max, is_debug)
    # else:
    #     send_email("Buzzing OK: No delay in kinesis data", p_diff_max, is_debug)


def run_i_hits_comparison(is_icrossing, is_debug):

    i_hits_flag, results = i_hits_main(is_icrossing,is_debug)

    #
    # if "OK" not in i_hits_flag:
    #     send_email("CRITICAL: Buzzing reconciliation with i_hits dropped", results, is_debug)
    # else:
    #     send_email("Buzzing OK: Reconciliation metrics with i_hits healthy", results, is_debug)


def main(period, is_debug, is_icrossing):
    if "," in period:
        periods = period.split(",")
        for p in periods:
            run_icrossing_comparison(p, is_debug, is_icrossing)
    else:
        run_icrossing_comparison(period, is_debug, is_icrossing)

    run_i_hits_comparison(is_icrossing, is_debug)
    run_delay_report(is_debug)


def run_icrossing_comparison(period, is_debug, is_icrossing):
    if not is_icrossing:
        return
    eval_final, msg_str = eval_final_main(period, is_debug)
    if "OK" not in eval_final:
        send_email("CRITICAL: Buzzing reconciliation dropped for period {period}".format(period=period), msg_str, is_debug)
    # else:
    #     send_email("Buzzing OK: Reconciliation metrics healthy for period {period}".format(period=period), msg_str, is_debug)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--period', dest='period', type=str, default='res5,res120,res840')
    parser.add_argument('--is_debug', dest='is_debug', type=bool, default=False)
    parser.add_argument('--is_icrossing', dest='is_icrossing', type=bool, default=True)

    args = parser.parse_args()
    main(args.period, args.is_debug, args.is_icrossing)



