import datetime
import getopt
import os
import subprocess
import threading
import platform
import pandas as pd

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

NULL = "/dev/null" if platform == "Linux" else "NUL"


def get_filesize_megabyte(inpath):
    return os.path.getsize(inpath) / 1024 / 1024


def make_dirs(indir):
    try:
        os.makedirs(indir, 0o0777)
    except Exception as e:
        print(e)
        return False
    print("Directory {0} doesn't exist, new directory made.".format(indir))
    return True


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


def generate_file(inpath, outpath, lock=None, semaphore=None, force_num=(-1, -1)):
    # inpath and outpath all single files

    filesize = get_filesize_megabyte(inpath)
    cmdl = "\"{0}\" --generate-only -i \"{1}\" -o \"{2}\" > {3}" \
        .format(exe_path, inpath, outpath, NULL)
    returncode = run_cmd(cmdl, base_time=filesize, lock=None, semaphore=semaphore)
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
                for i in range(len(ins)):
                    generate_file(ins[i], outs[i])
            else:
                # lock = threading.RLock()
                semaphore = threading.BoundedSemaphore(jobs)
                threads = []
                total = len(ins)
                for i in range(total):
                    t = threading.Thread(target=generate_file,
                                         args=(ins[i], outs[i], None, semaphore, (total - i, total),))
                    threads.append(t)
                cur_threads = []
                while len(threads) > 0:
                    if len(threading.enumerate()) <= jobs:
                        for i in range(jobs * 2):
                            cur_threads.append(threads.pop())
                            if len(threads) == 0:
                                break
                        for ts in cur_threads:
                            ts.start()
                        for tj in cur_threads:
                            tj.join()
                        cur_threads = []
                # for ts in threads:
                #     ts.start()
                # for tj in threads:
                #     tj.join()
            return True

        else:  # dir -> not dir
            print("Invalid output path.")
            return False
    else:
        print("Invalid output path.")
        return False


def verify_file(inpath, refpath, outpath, lock=None, semaphore=None, force_num=(-1, -1)):
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
                    verify_file(ins[i], refs[i], outs[i], lock=None, semaphore=None, force_num=(total - i, total))
            else:
                semaphore = threading.BoundedSemaphore(jobs)
                threads = []
                for i in range(total):
                    t = threading.Thread(
                        target=verify_file,
                        args=(ins[i], refs[i], outs[i], None, semaphore, (total - i, total),)
                    )
                    threads.append(t)
                cur_threads = []
                while len(threads) > 0:
                    if len(threading.enumerate()) <= jobs:
                        for i in range(jobs * 2):
                            cur_threads.append(threads.pop())
                            if len(threads) == 0:
                                break
                        for ts in cur_threads:
                            ts.start()
                        for tj in cur_threads:
                            tj.join()
                        cur_threads = []
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
            infos.append(count_diagnose(file))
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
    -------------------------------
    -j, --job               Specify number of parallel threads
    -t, --timeout-factor    Specify time limit to kill threads when time out
    '''

    input_path = output_path = ref_path = ""
    _flatten = _recursive = _generate = False
    _recursive = True
    _ref_flattened = False
    _use_config = False

    global jobs, timeout_factor, exe_path, print_info
    # input_path = "os.path.join(working_dir, "testcase")"
    exe_path = "C:\\Users\\Administrator\\source\\repos\\test3\\Release\\test3.exe"
    input_path = "C:\\Users\\Administrator\\Desktop\\新建文件夹\\图纸\\场景测试图纸"
    output_path = "C:\\Users\\Administrator\\Desktop\\新建文件夹\\Out"
    ref_path = "C:\\Users\\Administrator\\Desktop\\新建文件夹\\Ref"
    config_path = os.path.abspath("tasks.json")

    run_time = datetime.datetime.now()

    if argv:
        opts, args = getopt.getopt(argv[:1], "hi:", ["help", "input-path"])
        for o, a in opts:
            if o == "h" or o == "help":
                print(__doc__)
                exit(0)
            if o == "p" or o == "print":
                print_info = True
            elif o == "e" or o == "exec":
                exe_path = str(a)
            elif o == "I" or o == "input-path":
                input_path = str(a)
            elif o == "O" or o == "output-path":
                output_path = str(a)
            elif o == "R" or o == "ref-path":
                ref_path = str(a)
            elif o == "g" or o == "generate-only":
                _generate = True
            elif o == "r" or o == "recursive":
                _recursive = True
            elif o == "f" or o == "flatten":
                _flatten = True
            elif o == "ref-flattened":
                _ref_flattened = True
            elif o == "j" or o == "job":
                jobs = int(a)
            elif o == "t" or o == "timeout-factor":
                timeout_factor = float(a)
            elif o == "c" or o == "config":
                _use_config = True
                config_path = config_path if str(a) == "" else str(a)
                break

    if not (os.path.exists(exe_path) and os.path.exists(input_path)):
        print("Invalid arguments, executable or input path not exist.")
        exit(0)

    if _generate:
        return generate_files(input_path, output_path, flatten=_flatten, recursive=_recursive)
    else:
        res = verify_files(input_path, ref_path, output_path, ref_flattened=_ref_flattened, flatten=_flatten,
                           recursive=_recursive)
        diagnoses = count_diagnoses(output_path,
                                    recursive=(False if _flatten else True) and _recursive,
                                    user_filter=lambda file: datetime.datetime.fromtimestamp(
                                        os.path.getmtime(file)) > run_time
                                    )
        df = get_diagnoses_dataframe(diagnoses, ["trace", "status", "time"])
        try:
            df.to_csv("results.csv", encoding="utf-8-sig")
        except Exception as e:
            print(e)
            print("\033[1;31mCannot write to csv file. \033[0m")
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
        return res


if __name__ == '__main__':
    main()
