import datetime
import tarfile
import argparse
import os,sys
import shutil
from buzzing_evaluation import main
from print_evaluation_results import print_evaluation_results, print_graphite_command
BASELINE_BUCKET='s3://hearstkinesisdata/v4/'
NEW_BUCKET = "s3://hearstdataservices/buzzing/v4/"
cpath = os.path.dirname(os.path.realpath(__file__))

def copy_to_local(period,save=False):
    print("Downloading baseline data")
    os.system('aws s3 cp '+BASELINE_BUCKET + 'mediaos_json_files.tar.gz baseline_mediaos.tar.gz')
    print("Downloading peter data")
    os.system('aws s3 cp ' + NEW_BUCKET + 'mediaos_json_files.tar.gz new_mediaos.tar.gz')
    dt = datetime.datetime.utcnow()
    dt = dt.strftime("%Y-%m-%d_%H:%M")


    baseline_output_folder = os.path.realpath(os.path.join(cpath, "data/baseline/" + dt))
    new_output_folder = os.path.realpath(os.path.join(cpath, "data/peter/" + dt))

    # if not os.path.exists(baseline_output_folder):
    #     os.makedirs(baseline_output_folder)
    # if not os.path.exists(new_output_folder):
    #     os.makedirs(new_output_folder)
    #
    tar = tarfile.open("baseline_mediaos.tar.gz")
    tar.extractall(path=baseline_output_folder)
    tar.close()
    tar = tarfile.open("new_mediaos.tar.gz")
    tar.extractall(path=new_output_folder)
    tar.close()
    print("Running evaluation")
    ev = main(os.path.join(new_output_folder,period+'.json' ),os.path.join(baseline_output_folder,period+'.json' ))
    f = open("buzzing_"+str(dt)+".txt","w")
    orig_stdout = sys.stdout
    sys.stdout = f
    print_evaluation_results(ev)
    sys.stdout = orig_stdout
    f.close()

    print_graphite_command(ev)
    if not save:
        os.remove("baseline_mediaos.tar.gz")
        os.remove("new_mediaos.tar.gz")
        shutil.rmtree(baseline_output_folder)
        shutil.rmtree(new_output_folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buzzing Evaluations')


    parser.add_argument('--period',dest='period', type=str, default='res5')


    args = parser.parse_args()
    copy_to_local( args.period)