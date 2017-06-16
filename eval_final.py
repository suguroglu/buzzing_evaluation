#!/usr/bin/python3
import argparse
import datetime
import os
import shutil
import tarfile
from config import PROD_FINAL_OUTPUT_LOC, DEV_FINAL_OUTPUT_LOC
from compare_final_outputs import compare

# Import the email modules we'll need


cpath = os.path.dirname(os.path.realpath(__file__))


def main(period, is_debug, save=False):
    print("Downloading icrossing data")
    os.system('aws s3 cp ' + PROD_FINAL_OUTPUT_LOC + 'mediaos_json_files.tar.gz baseline_mediaos.tar.gz')
    print("Downloading parsely data")
    os.system('aws s3 cp ' + DEV_FINAL_OUTPUT_LOC + 'mediaos_json_files.tar.gz new_mediaos.tar.gz')
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
    alert, stats_str = compare(test_file, baseline_file, period, is_debug)

    if not save:
        os.remove("baseline_mediaos.tar.gz")
        os.remove("new_mediaos.tar.gz")
        shutil.rmtree(baseline_output_folder)
        shutil.rmtree(parsely_output_folder)

    if alert:
        print("ALERT: Reconciliation dropped")
        return "ALERT: Reconciliation dropped", stats_str

    return "OK", stats_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')

    parser.add_argument('--period', dest='period', type=str, default='res5')
    parser.add_argument('--is_debug', dest='debug', type=bool, default=False)
    args = parser.parse_args()
    main(args.period, args.is_debug)
