import smtplib
import json
from jinja2 import Environment, FileSystemLoader
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ts_analysis import main as ts_analysis_main
from eval_final import main as eval_final_main
import argparse
from_email = "suguroglu@hearst.com"
to_email = "hds-notifications@hearst.com"
#to_email = "suguroglu@hearst.com"

import datetime
dutc = datetime.datetime.utcnow()
begin_hour = dutc.strftime("%Y-%m-%d %H:%M:%S")

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
    delay_alert, p_diff_max = ts_analysis_main(is_redshift=False) # independent chron is running so write to redshift again
    if "OK" not in delay_alert:
        send_email("Buzzing Warning: Delay in buzzing kinesis", p_diff_max, is_debug)



def main(period, is_debug, is_icrossing):
    print("DEBUG: {debug}".format(debug=is_debug))
    print("RUNNING FOR {date}".format(date=begin_hour))
    if "," in period:
        periods = period.split(",")
        for p in periods:
            run_eval_final(p, is_debug, is_icrossing)
    else:
        run_eval_final(period, is_debug, is_icrossing)

    run_delay_report(is_debug)


def run_eval_final(period, is_debug, is_icrossing):
    if not is_icrossing:
        return
    eval_final, msg_str = eval_final_main(period, is_debug)
    if "OK" not in eval_final:
        send_email("CRITICAL: Buzzing reconciliation dropped for period {period}".format(period=period), msg_str, is_debug)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--period', dest='period', type=str, default='res5,res120,res840')
    parser.add_argument('--is_debug', dest='is_debug', type=bool, default=False)
    parser.add_argument('--is_icrossing', dest='is_icrossing', type=bool, default=True)

    args = parser.parse_args()
    main(args.period, args.is_debug, args.is_icrossing)



