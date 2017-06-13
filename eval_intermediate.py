import datetime
import argparse
import os,sys
from print_evaluation_results import print_evaluation_results, print_graphite_command
import shutil
from compare_intermediate_output import main
import gzip
BASELINE_BUCKET='s3://hearstkinesisdata/processedsparkjsonix/'
NEW_BUCKET = "s3://hearstdataservices/buzzing/parselyjson/"
cpath = os.path.dirname(os.path.realpath(__file__))

def copy_to_local(period_str,save=True):
    dt = datetime.datetime.utcnow()
    dt = dt.strftime("%Y/%m/07/22/60/50/")


    if not period_str:
        period_str = dt
    print("Downloading baseline data")
    os.system('aws s3 cp '+BASELINE_BUCKET+ period_str + 'part-00000.gz baseline_part.gz')
    print("Downloading parsely data")
    print(NEW_BUCKET + period_str+ 'part-00000.gz')
    os.system('aws s3 cp ' + NEW_BUCKET + period_str+ 'part-00000.gz parsely_part.gz')


    baseline_output_folder = os.path.realpath(os.path.join(cpath, "data/intermediate/" ))

    if not os.path.exists(baseline_output_folder):
        os.makedirs(baseline_output_folder)


    baselinename = os.path.join(baseline_output_folder,"baseline.txt")
    with gzip.open("baseline_part.gz", 'rb') as infile:
        with open(baselinename, 'wb') as outfile:
            for line in infile:
                outfile.write(line)

    parselyname = os.path.join(baseline_output_folder, "parsely.txt")
    with gzip.open("parsely_part.gz", 'rb') as infile:
        with open(parselyname, 'wb') as outfile:
            for line in infile:
                outfile.write(line)


    print("Running evaluation")
    print(parselyname)
    print(baselinename)
    ev = main(parselyname,baselinename)
    f = open("buzzing_intermediate_"+period_str.replace("/","_")+".txt","w")
    orig_stdout = sys.stdout
    sys.stdout = f
    print_evaluation_results(ev)
    sys.stdout = orig_stdout
    f.close()

    print_graphite_command(ev)
    if not save:
        os.remove("baseline_part.gz")
        os.remove("parsely_part.gz")
        shutil.rmtree(baseline_output_folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')


    parser.add_argument('--period',dest='period', type=str, default=None)


    args = parser.parse_args()
    copy_to_local( args.period)