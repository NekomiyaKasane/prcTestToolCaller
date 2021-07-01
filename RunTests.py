import datetime
import getopt
import os
import subprocess
import sys
import threading
import platform
import multiprocessing as mp
import time

import pandas as pd
import json

# import time
# import random
# import pathlib

working_dir = os.getcwd()
testcases_dir = os.path.join(working_dir, "testcase")
exe_path = os.path.join(working_dir, "stack.exe")
print("Working dir: {0}".format(working_dir))

timeout_factor = 600
jobs = 5
print_info = True
run_time = datetime.datetime.now()

NULL = "/dev/null" if platform == "Linux" else "NUL"


def get_filesize_megabyte(inpath):
    return os.path.getsize(inpath) / 1024 / 1024


def make_dirs(indir):
    if not os.path.isdir(indir):
        os.makedirs(indir, 0o0777)
        print("Directory {0} doesn't exist, new directory made.".format(indir))
        return True
    return False


def run_cmd(cmdl, base_time=1.0, lock=None, semaphore=None):
    if semaphore:
        semaphore.acquire()
    process = subprocess.Popen(args=cmdl, shell=True, stdin=None, stdout=None,
                               stderr=None)

    timer = threading.Timer(timeout_factor * base_time, process.kill)
    try:
        timer.start()
        process.communicate()
    finally:
        timer.cancel()

    if semaphore:
        semaphore.release()
    return process.returncode


def generate_file_wrapper(args):
    global exe_path
    exe_path = args[0]
    return generate_file(*(args[1:]))


def generate_file(inpath, outpath, force_num=(-1, -1)):
    # inpath and outpath all single files

    filesize = get_filesize_megabyte(inpath)
    cmdl = "\"{0}\" --generate-only -i \"{1}\" -o \"{2}\" > {3}" \
        .format(exe_path, inpath, outpath, NULL)
    returncode = run_cmd(cmdl, base_time=filesize)
    if print_info:
        if not returncode:
            info = "\033[0;31m Failed \033[0m"
        else:
            info = "\033[0;32m Succeeded \033[0m"
        print("\033[1;33m[{0}/{1}]{4}\033[0mGen: {2}\033[1;34m -> \033[0m{3}".format(force_num[0], force_num[1],
                                                                                     inpath, outpath, info))

    return not returncode


def generate_files(indir, outdir, recursive=True, flatten=False):
    if not os.path.exists(indir):
        print("Invalid input path.")
        return False
    elif os.path.isfile(indir):
        if not os.path.exists(outdir):  # file -> not existed, outdir as file path
            if not make_dirs(outdir):
                return False
        elif os.path.isfile(outdir):  # file -> existed file
            try:
                os.remove(outdir)
            except Exception as e:
                print(e)
                return False
            return generate_file(indir, outdir)
        if os.path.isdir(outdir):  # file -> existed path
            outpath = get_out_path(indir, outdir, extension=".json", flatten=True)
            return generate_file(indir, outpath)
        else:
            print("Invalid output path.")
            return False
    elif os.path.isdir(indir):
        if not os.path.exists(outdir):  # dir -> non-existed
            if not make_dirs(outdir):
                return False
        if os.path.isdir(outdir):  # dir -> dir
            ins, outs = get_out_paths(indir, outdir,
                                      in_ext=".dwg", out_ext=".json",
                                      recursive=recursive, flatten=flatten
                                      )
            if not flatten:
                for out in outs:
                    if not os.path.exists(os.path.dirname(out)):
                        os.makedirs(os.path.dirname(out))
            global jobs
            if jobs < 2:
                total = len(ins)
                for i in range(total):
                    generate_file(ins[i], outs[i], force_num=(i + 1, total))
            else:
                runner = mp.Pool(jobs)
                args = [(exe_path, ins[i], outs[i], (i + 1, len(ins))) for i in range(len(ins))]
                runner.map(generate_file_wrapper, args)
            return True

        else:  # dir -> not dir
            print("Invalid output path.")
            return False
    else:
        print("Invalid output path.")
        return False


def verify_file_wrapper(args):
    global exe_path
    exe_path = args[0]
    return verify_file(*(args[1:]))


def verify_file(inpath, refpath, outpath, force_num=(-1, -1)):
    # inpath, outpath and refpath are all single files
    cmdl = "\"{0}\" -i \"{1}\" -o \"{2}\" -r \"{3}\" > \"{4}\" 2>&1" \
        .format(exe_path, inpath, outpath, refpath, outpath)
    filesize = get_filesize_megabyte(inpath)
    returncode = run_cmd(cmdl, base_time=filesize)
    if print_info:
        if not returncode:
            info = "\033[0;31m Failed \033[0m"
        else:
            info = "\033[0;32m Succeeded \033[0m"
        print("\033[1;33m[{0}/{1}]{5}\033[0mVerify: {2} \033[1;34m && \033[0m{3}\033[1;34m -> \033[0m{4})"
              .format(force_num[0], force_num[1], inpath, refpath, outpath, info))
    return returncode


def verify_files(indir, refdir, outdir, ref_flattened=False, flatten=False, recursive=True):
    if os.path.isfile(indir):
        bare_filename = os.path.splitext(os.path.basename(indir))[0]
        if os.path.isdir(refdir):
            ref_file = os.path.join(refdir, bare_filename + ".json")
            if os.path.exists(ref_file):
                if os.path.isfile(outdir):  # file - dir - file
                    return verify_file(indir, ref_file, outdir)
                elif not os.path.exists(outdir):  # file - dir
                    if not make_dirs(outdir):
                        return False
                if os.path.isdir(outdir):  # file - dir - dir
                    return verify_file(indir, ref_file, os.path.join(outdir, bare_filename + ".trace"))
        elif os.path.isfile(refdir):
            if os.path.isfile(outdir):  # file - file - file
                return verify_file(indir, refdir, outdir)
            elif not os.path.exists(outdir):  # file - dir
                if not make_dirs(outdir):
                    return False
                print("Directory {0} doesn't exist, new directory made.".format(outdir))
            if os.path.isdir(outdir):  # file - file - dir
                return verify_file(indir, refdir, os.path.join(outdir, bare_filename + ".trace"))
    elif os.path.isdir(indir):
        if not os.path.exists(outdir):
            if not make_dirs(outdir):
                return False
        if os.path.isdir(refdir) and os.path.isdir(outdir):  # dir - dir - dir
            ins, refs = get_out_paths(indir, refdir,
                                      in_ext=".dwg", out_ext=".json",
                                      recursive=recursive, flatten=ref_flattened
                                      )
            outs = get_out_paths(indir, outdir,
                                 in_ext=".dwg", out_ext=".trace",
                                 recursive=recursive, flatten=flatten
                                 )[1]
            if not flatten:
                for out in outs:
                    if not os.path.exists(os.path.dirname(out)):
                        os.makedirs(os.path.dirname(out))

            total = len(ins)
            if jobs < 2:
                for i in range(total):
                    verify_file(ins[i], refs[i], outs[i], force_num=(i + 1, total))
            else:
                runner = mp.Pool(jobs)
                args = [(exe_path, ins[i], refs[i], outs[i], (i + 1, len(ins))) for i in range(len(ins))]
                runner.map(verify_file_wrapper, args)
        else:
            print("Invalid output or reference path.")
            return False
    else:
        print("Invalid input path.")
        return False


def get_all_files(indir, recursive=False, user_filter=lambda x: True):
    out_files = []
    if recursive:
        for root, dirs, files in os.walk(indir, topdown=True, followlinks=False):
            for file in files:
                path = os.path.normpath(os.path.join(root, file))
                if user_filter(path):
                    out_files.append(path)
    else:
        ll = os.listdir(indir)
        for item in ll:
            path = os.path.normpath(os.path.join(indir, item))
            if os.path.isfile(path) and user_filter(path):
                out_files.append(path)
    return out_files


def get_out_path(inpath, outdir, extension="", flatten=True, stem=""):
    fullname = os.path.basename(inpath)
    newname = fullname if extension == "" else os.path.splitext(fullname)[0] + extension
    if flatten:
        return os.path.normpath(os.path.join(outdir, newname))
    else:
        return os.path.normpath(os.path.join(os.path.dirname(inpath.replace(stem, outdir)), newname))


def get_out_paths(indir, outdir, in_ext="", out_ext="", recursive=True, flatten=True):
    files = get_all_files(indir,
                          recursive=recursive,
                          user_filter=lambda x: os.path.splitext(x)[1] == in_ext,
                          )
    outs = []
    for file in files:
        outs.append(get_out_path(file, outdir, extension=out_ext, flatten=flatten, stem=indir))
    return files, outs


def count_diagnose(inpath, prefix='>>>>', definer='::'):
    infos = {}
    if os.path.isfile(inpath):
        try:
            with open(inpath, 'r') as f:
                for line in f:
                    if line.startswith(prefix):
                        pair = line[line.rfind(prefix) + len(prefix):].strip()
                        key = pair[:pair.rfind(definer)].strip()
                        value = pair[pair.rfind(definer) + len(definer):].strip()
                        infos[key] = value
            f.close()
            infos['trace'] = inpath
        except Exception as e:
            print(e)
            return infos
    else:
        print("File {0} doesn't exist.".format(inpath))
    return infos


def count_diagnoses(indir, recursive=True, prefix='>>>>', definer='::', user_filter=lambda x: True):
    files = get_all_files(indir, recursive=recursive, user_filter=lambda path: os.path.splitext(path)[1] == '.trace')
    infos = []
    for file in files:
        if user_filter(file):
            infos.append(count_diagnose(file, prefix=prefix, definer=definer))
    return infos


def get_diagnoses_dataframe(dict_infos, needed_keys):
    table = {}
    for key in needed_keys:
        table[key] = []
    for dict_info in dict_infos:
        for key in needed_keys:
            if key in dict_info:
                table[key].append(dict_info[key])
            else:
                table[key].append(None)
    return pd.DataFrame(table)


def print_statistics(df):
    result = df["status"].value_counts().reindex(["failed", "succeed"], fill_value=0)
    successes = result["succeed"]
    fails = result["failed"]
    total = successes + fails
    total_time = df['time'].astype("float32").sum()
    succeed_time = df.loc[df["status"] == "succeed"]["time"].astype('float32').sum()
    failed_time = total_time - succeed_time
    print('''
    \033[1;032m[Succeed]\033[0m %8d \033[1;033mtime\033[0m %12.8f
    \033[1;031m[failed ]\033[0m %8d \033[1;033mtime\033[0m %12.8f
    \033[1;034m[total  ]\033[0m %8d \033[1;033mtime\033[0m %12.8f
        ''' % (successes, succeed_time, fails, failed_time, total, total_time)
          )


def run_task(task):
    if not ('executable' in task and 'input' in task and 'output' in task):
        return False
    global jobs, timeout_factor, exe_path, print_info, run_time
    run_time = datetime.datetime.now()
    exe_path = task['executable'] if 'executable' in task else exe_path
    _name = task['name'] if 'name' in task else str(run_time)
    _generate_csv = (task['export-csv'] if task['export-csv'] != '' else _name + 'csv') if 'export-csv' in task else ''
    _get_bool = lambda key, dic=task: dic[key] if key in dic else False
    _generate = _get_bool('generate-only')
    if not _generate and not 'reference' in task:
        print('Lack of reference param.')
        return False
    _in_r = _get_bool('recursive', dic=task['input'])
    _out_f = _get_bool('flatten', dic=task['input'])
    _in_dir = task['input']['path']
    _out_dir = task['output']['path']
    _ref_f = False
    _ref_dir = ""
    if not _generate:
        _ref_f = _get_bool('flatten', dic=task['reference'])
        _ref_dir = task['reference']['path']
    jobs = int(task['jobs']) if 'jobs' in task else 1
    timeout_factor = task['timeout-factor'] if 'timeout-factor' in task else 600
    print_info = _get_bool('print')
    _collect_keys = task['keys'] if _generate_csv and ('keys' in task) else ['status', 'time'] # default key

    print("\033[1;35mTask: {0} >> {1}\033[0m".format(_name, str(run_time)))

    if _generate:
        return generate_files(_in_dir, _out_dir, flatten=_out_f, recursive=_in_r)
    else:
        _res = verify_files(_in_dir, _ref_dir, _out_dir,
                            ref_flattened=_ref_f,
                            flatten=_out_f,
                            recursive=_in_r
                            )
        _diagnoses = count_diagnoses(_out_dir,
                                     recursive=(False if _out_f else True) and _in_r,
                                     user_filter=lambda file: datetime.datetime.fromtimestamp(
                                         os.path.getmtime(file)) > run_time
                                     )
        _df = get_diagnoses_dataframe(_diagnoses, _collect_keys)
        if _generate_csv and not _df.empty:
            _generate_csv = os.path.abspath(os.path.normpath(_generate_csv))
            try:
                if os.path.splitext(_generate_csv)[1] != '.csv':
                    make_dirs(_generate_csv)
                    _df.to_csv(os.path.join(_generate_csv, _name + '.csv'), encoding="utf-8-sig")
                else:
                    make_dirs(os.path.dirname(_generate_csv))
                    _df.to_csv(_generate_csv, encoding="utf-8-sig")
            except Exception as e:
                print(e)
                print("\033[1;31mCannot write to csv file. \033[0m")
            if {'status', 'time'} <= set(_df.keys()):
                print_statistics(_df)
        return _res


def main(argv=None):
    '''
    Usage: python RunTests.py [options]
    -h, --help              Print helps
    -p, --print             Print progress information while running
    -c, --config            Use tasks.json to generate tasks
    -E, --exec              Specify executable path
    -r, --recursive
    -f, --flatten
    -g, --generate-only     Generate
    -I, --input-path        Specify input files
    -O, --output-path       Specify output files, for generate mode, it's path for .json
                            while for verify mode, it's path for .trace
    -R, --ref-path          Specify reference files
    --ref-flattened         Assert reference files are flattened in one folder
    --export-csv            Enable and specify where to export verification results to csv.
    -------------------------------
    -j, --job               Specify number of parallel threads
    -t, --timeout-factor    Specify time limit to kill threads when time out
    '''

    input_path = output_path = ref_path = ""
    _flatten = _recursive = _generate = False
    _recursive = True
    _ref_flattened = False
    _use_config = False

    global jobs, timeout_factor, exe_path, print_info, run_time
    # input_path = "os.path.join(working_dir, "testcase")"

    config_path = os.path.abspath("tasks.json")

    task = {"name": "main", "keys": ["trace", "status", "time"]}

    if len(argv) == 1:
        return

    opts, args = getopt.getopt(argv[1:],
                               "hpE:I:O:R:grfj:t:c:",
                               ["help", "input-path=", "print", "exec=",
                                "output-path=", "ref-path=", "generate-only", "recursive",
                                "ref-flattened", "export-csv=", "jobs=", "export-csv",
                                "timeout-factor=", "config="]
                               )
    for o, a in opts:
        if o == "-h" or o == "--help":
            print(__doc__)
            exit(0)
        if o == "-p" or o == "--print":
            task['print'] = True
        elif o == "-E" or o == "--exec":
            task['executable'] = os.path.normpath(str(a))
        elif o == "-I" or o == "--input-path":
            if 'input' in task:
                task['input']['path'] = os.path.normpath(str(a))
            else:
                task['input'] = {'path': os.path.normpath(str(a))}
        elif o == "-O" or o == "--output-path":
            if 'output' in task:
                task['output']['path'] = os.path.normpath(str(a))
            else:
                task['output'] = {'path': os.path.normpath(str(a))}
        elif o == "-R" or o == "--ref-path":
            if 'reference' in task:
                task['reference']['path'] = os.path.normpath(str(a))
            else:
                task['reference'] = {'path': os.path.normpath(str(a))}
        elif o == "-g" or o == "--generate-only":
            task['generate-only'] = True
        elif o == "-r" or o == "--recursive":
            if 'input' in task:
                task['input']['recursive'] = True
            else:
                task['input'] = {'recursive': True}
        elif o == "-f" or o == "--flatten":
            if 'output' in task:
                task['output']['flatten'] = True
            else:
                task['output'] = {'flatten': True}
        elif o == "--ref-flattened":
            if 'reference' in task:
                task['reference']['flatten'] = True
            else:
                task['reference'] = {'flatten': True}
        elif o == "--export-csv":
            task['export-csv'] = os.path.normpath(str(a))
        elif o == "-j" or o == "--jobs":
            task['jobs'] = int(a)
        elif o == "-t" or o == "--timeout-factor":
            task['time-factor'] = float(a)
        elif o == "-c" or o == "--config":
            _use_config = True
            config_path = config_path if str(a) == "" else os.path.normpath(str(a))
            break

    if _use_config:
        if not os.path.isfile(config_path):
            print("\033[1;31mInvalid config file.")
            exit(1)
        else:
            f = open(config_path, 'r', encoding='utf-8')
            contents = json.load(f)
            f.close()
            tasks = contents['tasks'] if 'tasks' in contents else []
            for onetask in tasks:
                run_task(onetask)
    else:
        return run_task(task)


if __name__ == '__main__':
    main(sys.argv)
