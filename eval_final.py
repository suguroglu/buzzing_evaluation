import datetime
import tarfile
import argparse
import os, sys
import shutil
# from compare_final_output import main
from compare_final_2 import main as compare
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText

from print_evaluation_results import print_evaluation_results, print_graphite_command

BASELINE_BUCKET = 's3://hearstkinesisdata/v4/'
NEW_BUCKET = "s3://hearstdataservices/buzzing/v4/"
cpath = os.path.dirname(os.path.realpath(__file__))


def main(period="res5", save=False):
    print("Downloading icrossing data")
    os.system('aws s3 cp ' + BASELINE_BUCKET + 'mediaos_json_files.tar.gz baseline_mediaos.tar.gz')
    print("Downloading parsely data")
    os.system('aws s3 cp ' + NEW_BUCKET + 'mediaos_json_files.tar.gz new_mediaos.tar.gz')
    dt = datetime.datetime.utcnow()
    dt = dt.strftime("%Y-%m-%d_%H:%M")

    baseline_output_folder = os.path.realpath(os.path.join(cpath, "data/final/" + dt + "/icrossing"))
    parsely_output_folder = os.path.realpath(os.path.join(cpath, "data/final/" + dt + "/parsely"))

    if not os.path.exists(baseline_output_folder):
        os.makedirs(baseline_output_folder)
    if not os.path.exists(parsely_output_folder):
        os.makedirs(parsely_output_folder)

    tar = tarfile.open("baseline_mediaos.tar.gz")
    tar.extractall(path=baseline_output_folder)
    tar.close()

    tar = tarfile.open("new_mediaos.tar.gz")
    tar.extractall(path=parsely_output_folder)
    tar.close()

    print("Running evaluation")
    test_file = os.path.join(parsely_output_folder, period + '.json')
    baseline_file = os.path.join(baseline_output_folder, period + '.json')
    print("Test file: {test_file}".format(test_file=test_file))
    print("Baseline file: {baseline_file}".format(baseline_file=baseline_file))
    alert, stats_out_file = compare(test_file, baseline_file)

    if not save:
        os.remove("baseline_mediaos.tar.gz")
        os.remove("new_mediaos.tar.gz")
        shutil.rmtree(baseline_output_folder)
        shutil.rmtree(parsely_output_folder)

    if alert:
        print("ALERT: Reconciliation dropped")
        return "ALERT: Reconciliation dropped"

    return "OK"

    # f = open("buzzing_"+str(dt)+".txt","w")
    # orig_stdout = sys.stdout
    # sys.stdout = f
    # print_evaluation_results(ev)
    # sys.stdout = orig_stdout
    # f.close()
    #
    # print_graphite_command(ev)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--period', dest='period', type=str, default='res5')

    args = parser.parse_args()
    main(args.period)
