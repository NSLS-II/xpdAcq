#!/usr/bin/env python
##############################################################################
#
# xpdacq            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Dan Allan, Simon Billinge
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
import shutil
import subprocess
import sys
import typing as tp
from pathlib import Path
from time import strftime

import yaml
from IPython import get_ipython
from pkg_resources import resource_filename as rs_fn

from .beamtime import Beamtime, ScanPlan, Sample, ct
from .glbl import glbl
from .tools import _graceful_exit, xpdAcqError
from .xpdacq_conf import glbl_dict, _load_beamline_config

# list of exposure times for pre-poluated ScanPlan inside
# _start_beamtime
EXPO_LIST = [5, 0.1, 1, 10, 30, 60]
DATA_DIR = rs_fn("xpdacq", "data/")


def _start_beamtime(
    PI_last, saf_num, experimenters=[], wavelength=None, test=False
):
    """function for start a beamtime"""
    # check status first
    active_beamtime = glbl.get('_active_beamtime')
    if active_beamtime is False:
        raise xpdAcqError("It appears that end_beamtime may have been "
                          "run.\nIf you wish to start a new beamtime, "
                          "please open a new terminal and proceed "
                          "with the standard starting sequence.")
    # check directory
    home_dir = glbl_dict['home']
    if not os.path.exists(home_dir):
        raise RuntimeError(
            "WARNING: fundamental directory {} does not "
            "exist.\nPlease contact beamline staff immediately".format(
                home_dir
            )
        )

    f_list = os.listdir(home_dir)
    if len(f_list) != 0:
        raise FileExistsError(
            "There are more than one files/directories:\n"
            "{}\n"
            "under {}.\n"
            "have you run '_end_beamtime()' yet?".format(f_list, home_dir)
        )
    elif len(f_list) == 0:
        _make_clean_env()
        print("INFO: initiated requried directories for experiment")
        bt = Beamtime(PI_last, saf_num, experimenters, wavelength=wavelength)
        os.chdir(home_dir)
        print(
            "INFO: to link newly created beamtime object to xrun, "
            "please do\n"
            ">>> xrun.beamtime = bt"
        )
        # copy default Ni24.D to xpdUser/user_analysis
        src = os.path.join(DATA_DIR, "Ni24.D")
        dst = os.path.join(glbl_dict["usrAnalysis_dir"], "Ni24.D")
        shutil.copy(src, dst)
        _load_beamline_config(
            glbl["blconfig_path"], test=test
        )
        # pre-populated scan plan
        for expo in EXPO_LIST:
            ScanPlan(bt, ct, expo)
        # inject beamtime state
        glbl['_active_beamtime'] = True

        return bt


def _make_clean_env():
    """Make a clean environment for a new user
    """
    out = []
    for d in glbl_dict["allfolders"]:
        os.makedirs(d, exist_ok=True)
        out.append(d)
    return out


def start_xpdacq():
    """ function to reload beamtime """
    try:
        bt_list = [
            f
            for f in os.listdir(glbl_dict["yaml_dir"])
            if f.startswith("bt") and os.path.isfile(os.path.join(glbl_dict["yaml_dir"], f))
        ]
    except FileNotFoundError:
        return _no_beamtime()

    if len(bt_list) == 1:
        bt = load_beamtime()
        return bt

    elif len(bt_list) > 1:
        print(
            "WARNING: There are more than one beamtime objects:"
            "{}".format(bt_list)
        )
        print("Please contact beamline staff immediately")

    else:
        return _no_beamtime()


def _no_beamtime():
    print("INFO: No beamtime object has been found")
    print(
        "INFO: Please run 'bt=_start_beamtime(<PI_last>, <saf_num>,"
        "<experimenter_list>, wavelength=<wavelength_num>)'"
        "to initiate beamtime"
    )


def load_beamtime(directory=None):
    """
    Load a Beamtime and associated objects.

    Expected directory structure:

    <glbl['yaml_dir']>/
      bt_bt.yml
      glbl.yml
      samples/
      scanplans/
    """
    if directory is None:
        directory = glbl_dict["yaml_dir"]  # leave room for multi-beamtime
    known_uids = {}
    beamtime_fn = os.path.join(directory, "bt_bt.yml")
    sample_fns = [fn for fn in os.listdir(os.path.join(directory, "samples"))]
    scanplan_fns = [
        fn for fn in os.listdir(os.path.join(directory, "scanplans"))
    ]

    with open(beamtime_fn, "r") as f:
        bt = load_yaml(f, known_uids)

    # most recent scanplan order
    scanplan_order_fn = os.path.join(
        glbl_dict["config_base"], ".scanplan_order.yml"
    )
    if os.path.isfile(scanplan_order_fn):
        with open(scanplan_order_fn) as f:
            scanplan_order = yaml.unsafe_load(f)
        for fn in sorted(
            scanplan_fns, key=list(scanplan_order.values()).index
        ):
            with open(os.path.join(directory, "scanplans", fn), "r") as f:
                load_yaml(f, known_uids)
    # most recent sample order
    sample_order_fn = os.path.join(
        glbl_dict["config_base"], ".sample_order.yml"
    )
    if os.path.isfile(sample_order_fn):
        with open(sample_order_fn) as f:
            sample_order = yaml.unsafe_load(f)
        for fn in sorted(sample_fns, key=list(sample_order.values()).index):
            with open(os.path.join(directory, "samples", fn), "r") as f:
                load_yaml(f, known_uids)

    return bt


def load_yaml(f, known_uids=None):
    """
    Recreate a ScanPlan, Experiment, or Beamtime object from a YAML file.

    If its linked objects have already been created, re-link to them.
    If they have not yet been created, create them now.
    """
    if known_uids is None:
        known_uids = {}
    data = yaml.unsafe_load(f)
    # If f is a file handle, 'rewind' it so we can read it again.
    if not isinstance(f, str):
        f.seek(0)
    if isinstance(data, dict) and "bt_uid" in data:
        obj = Beamtime.from_yaml(f)
        known_uids[obj["bt_uid"]] = obj
        return obj
    elif isinstance(data, list) and "sa_uid" in data[0]:
        beamtime = known_uids.get(data[1]["bt_uid"])
        obj = Sample.from_yaml(f, beamtime=beamtime)
        known_uids[obj["sa_uid"]] = obj
    elif isinstance(data, list) and len(data) == 2:
        # elif isinstance(data, list) and 'sp_uid' in data[0]:
        beamtime = known_uids.get(data[1]["bt_uid"])
        obj = ScanPlan.from_yaml(f, beamtime=beamtime)
        known_uids[obj["sp_uid"]] = obj
    else:
        raise ValueError("File does not match a recognized specification.")
    return obj


def _end_beamtime(base_dir=None, archive_dir=None, bto=None, usr_confirm="y"):
    """Helper funciton to end a beamtime.

    Detail steps are:
        2) Archive ``xpdUser`` directory to remove backup
        3) Ask for user's confirmation
        4.1) if user confirms, flush all sub-directories under
        ``xpdUser`` for a new beamtime.
        4.2) if user doesn't confirm, leave ``xpdUser`` untouched
        and flush remote backup to avoid duplicate archives.
    """
    # NOTE: to avoid network bottleneck, we actually only move all files
    # except for .tif.

    _required_info = ["bt_piLast", "bt_safN", "bt_uid"]
    if archive_dir is None:
        archive_dir = glbl_dict["archive_dir"]
    if base_dir is None:
        base_dir = glbl_dict["base"]
    # check env
    if os.path.isdir(glbl_dict["home"]):
        files = os.listdir(glbl_dict["home"])
        if len(files) == 0:
            raise FileNotFoundError(
                "It appears that end_beamtime may have been "
                "run. If so, do not run again but proceed to\n"
                ">>> bt = _start_beamtime(pi_last, saf_num,"
                "experimenters, wavelength=<value>)\n"
            )
    # laod bt yaml
    ips = get_ipython()
    if not bto:
        # bto = _load_bt(glbl.yaml_dir)
        bto = ips.ns_table["user_global"]["bt"]
    # load bt info
    archive_name = _load_bt_info(bto, _required_info)
    # update beamtime state
    glbl['_active_beamtime'] = False
    # archive file
    archive_full_name, local_archive_name = _tar_user_data(archive_name)
    # confirm archive
    _confirm_archive(archive_full_name)
    # flush
    _delete_local_archive(local_archive_name)


def _clean_info(obj):
    """ stringtify and replace space"""
    return str(obj).strip().replace(" ", "")


def _load_bt_info(bt_obj, required_fields):
    # grab information
    bt_info_list = []
    for el in required_fields:
        # print('loaded bt info = {}'.format(dict(bt_obj)))
        bt_info = bt_obj.get(el)
        if bt_info is None:
            print(
                "WARNING: required beamtime information {} doesn't exist. "
                "User might have edited it during experiment. "
                "Please contact user for further inforamtion".format(el)
            )
            sys.exit()
        bt_info_list.append(_clean_info(bt_info))
    bt_info_list.append(strftime("%Y-%m-%d-%H%M"))
    archive_name = "_".join(bt_info_list)
    return archive_name


def _tar_user_data(archive_name, root_dir=None, archive_format="tar"):
    """ Create a remote tarball of all user folders under xpdUser directory
    """
    remote_archive_name = os.path.join(glbl_dict["archive_dir"],
                                       archive_name)
    if root_dir is None:
        root_dir = glbl_dict["base"]
    try:
        os.chdir(root_dir)
        # rename xpdUser to <xpdUser_archivefallname>
        src = glbl_dict['home']
        local_archive_name = '_'.join([src, archive_name])
        local_archive_name = shutil.move(src, local_archive_name)
        # make a fresh xpdUser dir. raise error if renaming wasn't successful
        os.makedirs(src)
        assert not os.listdir(src)  # assert empty
        print("INFO: Archiving your data now. That may take several"
              " minutes. Please be patient :)")
        # remove dir structure would be:
        # <remote>/<PI_last+uid>/xpdUser/....
        os.makedirs(remote_archive_name, exist_ok=True)
        subprocess.run([
            'rsync', '-av', '--timeout=60',
            '--no-owner', '--no-group',
            '--exclude=.*',  # exclude all hidden files
            local_archive_name, remote_archive_name
        ],
            check=True, )
    finally:
        # move back to fresh xpdUser
        os.chdir(src)
    return remote_archive_name, local_archive_name


def _load_bt(bt_yaml_path):
    btoname = os.path.join(glbl_dict["yaml_dir"], "bt_bt.yml")
    if not os.path.isfile(btoname):
        sys.exit(
            _graceful_exit(
                "{} does not exist in {}. User might have"
                "deleted it accidentally.Please create it"
                "based on user information or contact user".format(
                    os.path.basename(btoname), glbl_dict["yaml_dir"]
                )
            )
        )
    with open(btoname, "r") as f:
        bto = yaml.unsafe_load(f)
    return bto


def _get_user_confirmation():
    conf = input(
        "Please confirm data are backed up.\n"
        "Are you ready to continue with xpdUser "
        "directory contents deletion (y,[n])?: "
    )
    return conf


def _any_input_method(inp_func):
    return inp_func()


def _confirm_archive(archive_f_name):
    print("tarball archived to {}".format(archive_f_name))
    conf = _any_input_method(_get_user_confirmation)
    if conf in ("y", "Y"):
        return
    else:
        # flush remote backup
        shutil.rmtree(archive_f_name)
        sys.exit(
            _graceful_exit(
                "xpdUser directory delete operation "
                "cancelled at Users request."
            )
        )


def _delete_local_archive(dir_to_flush: str) -> None:
    dir_path = Path(dir_to_flush)
    while dir_path.is_dir():
        shutil.rmtree(dir_path, onerror=handle_error)
    return


def handle_error(func: tp.Callable, path: str, exc_info: str) -> None:
    print(
        "INFO: Following files are unable to be deleted, maybe they are still used by current "
        "process, hence _end_beatime can't be done\n",
        path
    )
    print("Below is the exception message.")
    print(exc_info)
    msg = input(
        "INFO: If files are properly closed, "
        "please hit any key to continue the _end_beamtime "
        "process."
    )
    if msg:
        func(path)
    return


""" hodding place
# advanced version, allowed multiple beamtime. but not used for now
def _start_xpdacq():
    dirs = [d for d in os.listdir(glbl.yaml_dir) if os.path.isdir(d)]
    # create sample dir if it doesn't exist yet
    if 'samples' not in dirs:
        sample_dir = os.path.join(glbl.xpdconfig, 'samples')
        os.makedirs(sample_dir, exist_ok=True)
    else:
        dirs.remove('samples')
    # now we have all dirs that are not samples;
    # only PI_name dirs left

    if len(dirs) == 1:
        load_dirs = dirs[-1]
        bt = load_beamtime(load_dirs)

    elif len(dirs) > 1:
        print("INFO: There are more than one PI_name dirs:"
              "{}".format(dirs))
        print("Please choose the one you want to use and run:"
              "bt = load_beamtime(<path to the PI name dir>)")

    else:
        print("INFO: No PI_name has been found")
"""
