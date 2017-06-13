import os, json, gzip
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