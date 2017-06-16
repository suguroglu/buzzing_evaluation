import smtplib
import json
from jinja2 import Environment, FileSystemLoader
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ts_analysis import main as ts_analysis_main
from eval_final import main as eval_final_main
from i_hits_reconciliation import main as i_hits_main

from_email = "suguroglu@hearst.com"
to_email = "hds-notifications@hearst.com"


env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')))
template = env.get_template('email_template.html')

IS_ICROSSING = True
IS_DEBUG = True
period = "res5"


def send_email(title_str, msg_str, is_debug):
    msg = MIMEMultipart('alternative')

    if msg_str:
        msg_dict = json.loads(msg_str)
        rendered_email = template.render(results=msg_dict, title=title_str)
        part2 = MIMEText(rendered_email, 'html')
        msg.attach(part2)

    msg['Subject'] = title_str
    msg['From'] = from_email
    msg['To'] = to_email
    # s = smtplib.SMTP("localhost")
    # if not is_debug:
    #    s.sendmail(from_email, [to_email], msg.as_string())
    # s.quit()


delay_alert = ts_analysis_main()

if "OK" not in delay_alert:
    send_email("Buzzing Warning: Delay in buzzing kinesis", "", IS_DEBUG)
else:
    send_email("Buzzing OK: No delay in kinesis data", "", IS_DEBUG)


i_hits_final = i_hits_main(IS_ICROSSING,IS_DEBUG)


if IS_ICROSSING:
    eval_final, msg_str = eval_final_main(period, IS_DEBUG)
    if "OK" not in eval_final:
        send_email("CRITICAL: Buzzing reconciliation dropped", msg_str, IS_DEBUG)
    else:
        send_email("Buzzing OK: Reconciliation metrics healthy", msg_str, IS_DEBUG)
