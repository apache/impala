from __future__ import print_function
import hashlib
import multiprocessing.pool
import os
import re
import sys
from random import randint
from time import sleep
import subprocess

NUM_DOWNLOAD_ATTEMPTS = 8

PYPI_MIRROR = os.environ.get('PYPI_MIRROR', 'https://pypi.python.org')

# The requirement files that list all of the required packages and versions.
REQUIREMENTS_FILES = ['requirements.txt', 'setuptools-requirements.txt',
                      'kudu-requirements.txt', 'adls-requirements.txt']

python_deps_dir = "{0}/infra/python/deps".format(os.environ.get("IMPALA_HOME"))


def get_download_deps_info():
    with open("{0}/download_deps_info.txt".format(os.environ.get("IMPALA_HOME")), "w") as f:
        for requirements_file in REQUIREMENTS_FILES:
            for line in open("{0}/{1}".format(python_deps_dir, requirements_file)):
                line = line.split("#")[0]
                l = line.split(";")[0].strip()
                if not l:
                    continue
                # format: `pkg_name`==`pkg_version`
                f.write("python_dep##{0}\n".format(l))

    os.system("{0}/bin/bootstrap_toolchain.py fetch_download_info".format(os.environ.get("IMPALA_HOME")))


def check_digest(filename, algorithm, expected_digest):
    try:
        supported_algorithms = hashlib.algorithms_available
    except AttributeError:
        # Fallback to hardcoded set if hashlib.algorithms_available doesn't exist.
        supported_algorithms = set(['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512'])
    if algorithm not in supported_algorithms:
        print('Hash algorithm {0} is not supported by hashlib'.format(algorithm))
        return False
    h = hashlib.new(algorithm)
    h.update(open(filename).read())
    actual_digest = h.hexdigest()
    return actual_digest == expected_digest


def retry(func):
    '''Retry decorator.'''

    def wrapper(*args, **kwargs):
        for try_num in range(NUM_DOWNLOAD_ATTEMPTS):
            if try_num > 0:
                sleep_len = randint(5, 10 * 2 ** try_num)
                print('Sleeping for {0} seconds before retrying'.format(sleep_len))
                sleep(sleep_len)
            try:
                result = func(*args, **kwargs)
                if result:
                    return result
            except Exception as e:
                print(e)
        print('Download failed after several attempts.')
        sys.exit(1)

    return wrapper


def get_package_info(pkg_name, pkg_version):
    '''Returns the file name, path, hash algorithm and digest of the package.'''
    # We store the matching result in the candidates list instead of returning right away
    # to sort them and return the first value in alphabetical order. This ensures that the
    # same result is always returned even if the ordering changed on the server.
    candidates = []
    url = '{0}/simple/{1}/'.format(PYPI_MIRROR, pkg_name)
    print('Getting package info from {0}'.format(url))
    # The web page should be in PEP 503 format (https://www.python.org/dev/peps/pep-0503/).
    # We parse the page with regex instead of an html parser because that requires
    # downloading an extra package before running this script. Since the HTML is guaranteed
    # to be formatted according to PEP 503, this is acceptable.
    pkg_info = subprocess.check_output(["wget", "-q", "-O", "-", url])
    regex = r'<a .*?href=\".*?packages/(.*?)#(.*?)=(.*?)\".*?>(.*?)<\/a>'
    for match in re.finditer(regex, pkg_info):
        path = match.group(1)
        hash_algorithm = match.group(2)
        digest = match.group(3)
        file_name = match.group(4)
        # Make sure that we consider only non Wheel archives, because those are not supported.
        if (file_name.endswith('-{0}.tar.gz'.format(pkg_version)) or
                file_name.endswith('-{0}.tar.bz2'.format(pkg_version)) or
                file_name.endswith('-{0}.zip'.format(pkg_version))):
            candidates.append((file_name, path, hash_algorithm, digest))
    if not candidates:
        print('Could not find archive to download for {0} {1}'.format(pkg_name, pkg_version))
        return (None, None, None, None)
    return sorted(candidates)[0]


@retry
def download_python_package(pkg_name, pkg_version, download_directory):
    file_name, path, hash_algorithm, expected_digest = get_package_info(pkg_name,
                                                                        pkg_version)
    if not file_name:
        return False
    pkg_url = '{0}/packages/{1}'.format(PYPI_MIRROR, path)
    print('(python)Downloading {0} from {1}'.format(file_name, pkg_url))
    with open("{0}/python_deps_index.txt".format(download_directory), "a+") as f:
        subprocess.check_call(["wget", pkg_url, "-q", "-O", "{0}/{1}".format(download_directory, file_name)])
        if not check_digest("{0}/{1}".format(download_directory, file_name), hash_algorithm, expected_digest):
            raise Exception(
                'Hash digest check failed in file {0}.'.format("{0}/{1}".format(download_directory, file_name)))
        f.write("{0}_{1}##{2}\n".format(pkg_name, pkg_version, file_name))
    return True


@retry
def download_toolchain_package(toolchain_name, url, download_directory):
    print('(toolchain)Downloading {0} from {1}'.format(toolchain_name, url))
    subprocess.check_call(["wget", url, "-q", "--directory-prefix={0}".format(download_directory)])
    return True


if __name__ == '__main__':
    if len(sys.argv) == 4 and sys.argv[1].__eq__("download") and os.path.isfile(sys.argv[2]) and os.path.isdir(
            sys.argv[3]):
        deps_info_path = sys.argv[1]
        download_directory = sys.argv[2]
        results = []
        pool = multiprocessing.pool.ThreadPool(processes=min(multiprocessing.cpu_count(), 4))
        if os.listdir(download_directory):
            raise Exception("The directory is not empty.")
        python_deps_download_directory = "{0}/python_deps".format(download_directory)
        toolchain_deps_download_directory = "{0}/toolchain_deps".format(download_directory)
        os.mkdir(python_deps_download_directory)
        os.mkdir(toolchain_deps_download_directory)
        for line in open(deps_info_path):
            if line.startswith("python_dep"):
                pkg_name, pkg_version = line.split("##")[1].split("==")
                results.append(pool.apply_async(
                    download_python_package,
                    args=[pkg_name.strip(), pkg_version.strip(), python_deps_download_directory]))
            elif line.startswith("toolchain_dep"):
                toolchain_name, url = line.split("##")[1].split(" ")
                results.append(pool.apply_async(
                    download_toolchain_package,
                    args=[toolchain_name.strip(), url.strip(), toolchain_deps_download_directory]))
        for x in results:
            x.get()

    elif len(sys.argv) == 2 and sys.argv[1].__eq__("fetch_download_info"):
        get_download_deps_info()

    else:
        raise ("Incorrect input parameter.")
