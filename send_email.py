# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText
from ts_analysis import main as ts_analysis_main
from eval_final import main as eval_final_main
from i_hits_reconciliation import main as i_hits_main
from_email = "suguroglu@hearst.com"
to_email = "suguroglu@hearst.com"


def send_email(msg_str,title):
    msg = MIMEText(msg_str)
    msg['Subject'] = title
    msg['From'] = from_email
    msg['To'] = to_email
    s = smtplib.SMTP('localhost')
    s.sendmail(from_email, [to_email], msg.as_string())
    s.quit()

delay_alert = ts_analysis_main()
if "OK" not in delay_alert:
    send_email(delay_alert,"Buzzing Warning: Delay in buzzing kinesis")


eval_final = eval_final_main()
if "OK" not in eval_final:
    send_email(eval_final,"CRITICAL: Buzzing reconciliation dropped")

if "OK" not in i_hits_final:
    send_email(eval_final,"CRITICAL: Buzzing reconciliation dropped")

i_hits_final = i_hits_main()
