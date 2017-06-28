import os
from send_email import send_email

is_debug = False

try:
    os.system(
        "aws s3 ls --summarize --human-readable s3://hearstdataservices/buzzing/v4/mediaos_json_files.tar.gz > out.txt")

    f = open("out.txt")
    line = f.readline()
    f.close()
    size = line.split()[2]
except:
    size = -1

mdict = {"Current file size": size}
if float(size) < 4.:
    send_email("BUZZING ALERT: OUTPUT FILE SIZE IS TOO SMALL", mdict, is_debug)
else:
    print("ALL OK")
