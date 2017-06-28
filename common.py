import os, json, gzip,shutil

def load_data(bucket_name, period_str, delete=True):
    try:
        cp_command = 'aws s3 cp ' + bucket_name + period_str + 'part-00000.gz temp.gz'
        print(cp_command)
        os.system(cp_command)
        with gzip.open("temp.gz", 'rb') as infile:

            test_lines = [json.loads(el) for el in infile.readlines()]
        if delete:
            os.remove("temp.gz")
        return test_lines
    except:
        return []

def load_data_new(bucket_name, period_str, delete=True):
    cp_command = 'aws s3 cp --recursive ' + bucket_name + period_str + ' test/'
    os.system(cp_command)
    results = []
    recurse_path("test/",results)
    if delete:
        shutil.rmtree("test/")
    return results


def recurse_path(cp,results):
    if not os.path.isfile(cp):
        files = os.listdir(cp)
        for f in files:

            k = os.path.join(cp,f)
            recurse_path(k,results)
    else:
        if "part-00000.gz" in cp:
            with gzip.open(cp, 'rb') as infile:
                results += [el.decode("utf-8") for el in  infile.readlines()]


