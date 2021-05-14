#!/usr/bin/python3
import argparse
import atexit
from pyVim.connect import SmartConnectNoSSL, Disconnect
from pyVmomi import vim
from pyVim.task import WaitForTask
import json
import os
from pathlib import Path


_VERSION = 0.1
_DESCRIPTION = "ESXi control utility"


# General function and definitions
class EctlException(Exception):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return self.error


def read_config_file(args):
    if args.config is None:
        config_path = "{}/.local/config/ectl.json".format(str(Path.home()))
    else:
        config_path = args.config
    if not os.path.exists(config_path):
        return None
    try:
        return json.load(open(config_path))
    except (IOError, TypeError, ValueError) as e:
        raise EctlException("Error parsing {} - '{}'".format(config_path, e))


def exec_task(task, error_message, *task_args):
    if WaitForTask(task(*task_args)) != "success":
        raise EctlException(error_message)


def _determine_setting(arg, config, name):
    if arg is not None:
        return arg
    if config is None:
        return None
    try:
        return config[name]
    except KeyError:
        return None


def esxi_connect(config, args):
    host = _determine_setting(args.host, config, "host")
    user = _determine_setting(args.user, config, "user")
    pwd = _determine_setting(args.password, config, "password")
    if host is None:
        raise EctlException("Missing host setting")
    if user is None:
        raise EctlException("Missing user setting")
    if pwd is None:
        raise EctlException("Missing password setting")
    try:
        si = SmartConnectNoSSL(host=host, user=user, pwd=pwd)
        atexit.register(Disconnect, si)
        return si
    except vim.fault.InvalidLogin as e:
        raise EctlException("Invalid login details - '{}'".format(e))
    except Exception as e:
        raise EctlException("Error connecting to ESXi server - '{}'".format(e))


_MAX_DEPTH = 10
_VM_PRINT_FMT = "{:<60}{:<8}{:<18}"


# VM level functions and definitions

def print_vm_info(vm, depth=1):
    # if this is a group it will have children. if it does, recurse into them
    # and then return
    if hasattr(vm, 'childEntity'):
        if depth > _MAX_DEPTH:
            return
        vm_list = vm.childEntity
        for child in vm_list:
            print_vm_info(child, depth + 1)
        return

    summary = vm.summary
    power = summary.runtime.powerState
    ip = None
    if power in ["poweredOn", "poweredOff"]:
        power = power[7:]
        if power == "On":
            ip = summary.guest.ipAddress
    else:
        power = "n/a"
    if ip is None:
        ip = "n/a"

    print(_VM_PRINT_FMT.format(summary.config.name, power, ip))


def print_vm_list(si):
    print(_VM_PRINT_FMT.format("Name", "Power", "IP Address"))
    print(_VM_PRINT_FMT.format("----", "-----", "----------"))
    content = si.RetrieveContent()
    for child in content.rootFolder.childEntity:
        if hasattr(child, 'vmFolder'):
            data_center = child
            vm_folder = data_center.vmFolder
            vm_list = vm_folder.childEntity
            for vm in vm_list:
                print_vm_info(vm)


def find_vm(si, vm_name, raise_on_missing=True):
    content = si.RetrieveContent()
    container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    for c in container.view:
        if c.name == vm_name:
            return c
    if raise_on_missing:
        raise EctlException("Unable to find VM name {}".format(vm_name))
    return None


def start_vm(si, args):
    vm = find_vm(si, args.vm_name)
    vm.PowerOn()


def stop_vm(si, args):
    vm = find_vm(si, args.vm_name)
    vm.PowerOff()


def reset_vm(si, args):
    vm = find_vm(si, args.vm_name)
    vm.Reset()


# Snapshots functions and definitions

class SnapWorker(object):
    def __init__(self):
        self.cont_iteration = True

    def apply(self, snap):
        pass


def _snap_iterator(snap, worker):
    worker.apply(snap)
    if not worker.cont_iteration:
        return
    for snap in snap.childSnapshotList:
        _snap_iterator(snap, worker)


def snap_iterator(vm, worker):
    if vm.snapshot is None:
        return
    for snap in vm.snapshot.rootSnapshotList:
        _snap_iterator(snap, worker)


_SNAP_PRINT_FMT = "{:<6}{:<25}{:<30}{:<13}"


class SnapPrinter(SnapWorker):
    def __init__(self, vm):
        super(SnapPrinter, self).__init__()
        self.curr_snap = None
        if vm.snapshot is not None:
            self.curr_snap = vm.snapshot.currentSnapshot

    def apply(self, snap):
        curr = "*" if snap.snapshot == self.curr_snap else ""
        print(_SNAP_PRINT_FMT.format(snap.id, snap.name, snap.createTime.strftime("%c"), curr))


def print_snapshots(si, args):
    vm = find_vm(si, args.vm_name)
    if vm.snapshot is None:
        print("No snapshots found for vm '{}'", args.vm_name)
        return
    print(_SNAP_PRINT_FMT.format("ID", "Name", "Creation Time", "Crnt"))
    print(_SNAP_PRINT_FMT.format("--", "----", "-------------", "----"))
    snap_iterator(vm, SnapPrinter(vm))


class SnapFinder(SnapWorker):
    BY_NAME = 1
    BY_ID = 2

    def __init__(self, identifier, identifier_type):
        super(SnapFinder, self).__init__()
        self.identifier_type = identifier_type
        if self.identifier_type == self.BY_NAME:
            self.identifier = identifier
        else:
            try:
                self.identifier = int(identifier)
            except ValueError:
                raise EctlException("Illegal snap id - '{}'".format(identifier))
        self.snaps = []

    def apply(self, snap):
        if self.identifier_type == SnapFinder.BY_NAME:
            if snap.name == self.identifier:
                self.snaps.append(snap)
        else:  # BY_ID
            if snap.id == self.identifier:
                self.snaps.append(snap)
                self.cont_iteration = False


def find_snap(vm, args):
    if args.by_id:
        snap_finder = SnapFinder(identifier=args.snap, identifier_type=SnapFinder.BY_ID)
    else:
        snap_finder = SnapFinder(identifier=args.snap, identifier_type=SnapFinder.BY_NAME)
    snap_iterator(vm, snap_finder)
    if len(snap_finder.snaps) == 0:
        raise EctlException("VM '{}' has no such snapshot - '{}'".format(vm.summary.config.name, args.snap))
    if len(snap_finder.snaps) > 1:
        raise EctlException("VM '{}' has multiple snapshots named '{}', use snapshot ID".format(
            vm.summary.config.name, args.snap))
    return snap_finder.snaps[0]


def snap_create(si, args):
    vm = find_vm(si, args.vm_name)
    exec_task(vm.CreateSnapshot, "Error creating snapshot", args.snap, "", False, False)


def snap_remove(si, args):
    vm = find_vm(si, args.vm_name)
    snap = find_snap(vm, args)
    exec_task(snap.snapshot.RemoveSnapshot_Task, "Error removing snapshots", True)


def snap_revert(si, args):
    vm = find_vm(si, args.vm_name)
    snap = vm.snapshot.currentSnapshot if args.snap is None else find_snap(vm, args).snapshot
    exec_task(snap.RevertToSnapshot_Task, "Error reverting snapshot")
    if args.start:
        vm.PowerOn()


def snap_raw_info(si, args):
    vm = find_vm(si, args.vm_name)
    snap = find_snap(vm, args)
    print(snap)


class CurrSnapFinder(SnapWorker):
    def __init__(self, snap_ref):
        super(CurrSnapFinder, self).__init__()
        self.snap_ref = snap_ref
        self.curr_snap = None

    def apply(self, snap):
        if snap.snapshot == self.snap_ref:
            self.curr_snap = snap
            self.cont_iteration = False


def get_curr_snap(vm):
    snap_finder = CurrSnapFinder(vm.snapshot.currentSnapshot)
    snap_iterator(vm, snap_finder)
    return snap_finder.curr_snap


parser = argparse.ArgumentParser(description=_DESCRIPTION)
parser.set_defaults(need_args=True)
parser.add_argument('--version', action='version', version="{} v{}".format(_DESCRIPTION, _VERSION))
parser.add_argument("-c", "--config", help="Configuration file path", default=None)
parser.add_argument("-H", "--host", help="ESXi host to connect to", default=None)
parser.add_argument("-u", "--user", help="ESXi host user name", default=None)
parser.add_argument("-p", "--password", help="ESXi host user's password", default=None)

# Virtual Machines
vm_base_parser = argparse.ArgumentParser(add_help=False)
vm_base_parser.add_argument("vm_name", metavar="VM_NAME")

subparsers = parser.add_subparsers(required=True, title="command", dest="command")
vm_list_parser = subparsers.add_parser("vm-list", help="List virtual machines on server")
vm_list_parser.set_defaults(func=print_vm_list)
vm_list_parser.set_defaults(need_args=False)

start_parser = subparsers.add_parser("start", help="Start VM", parents=[vm_base_parser])
start_parser.set_defaults(func=start_vm)

stop_parser = subparsers.add_parser("stop", help="Stop VM", parents=[vm_base_parser])
stop_parser.set_defaults(func=stop_vm)

stop_parser = subparsers.add_parser("reset", help="Reset VM", parents=[vm_base_parser])
stop_parser.set_defaults(func=reset_vm)

snap_list_parser = subparsers.add_parser("snap-list", help="List snapshots for VM", parents=[vm_base_parser])
snap_list_parser.set_defaults(func=print_snapshots)


# Snapshots
snap_arg_base_parser = argparse.ArgumentParser(add_help=False)
snap_arg_base_parser.add_argument("snap", metavar="SNAP")
snap_search_base_parser = argparse.ArgumentParser(add_help=False)
snap_search_base_parser.add_argument("-i", "--by-id", help="Treat snapshot argument as snap ID", action="store_true",
                                     default=False)

snap_create_parser = subparsers.add_parser("snap-create", help="Create a VM to snapshot",
                                           parents=[vm_base_parser, snap_arg_base_parser, snap_arg_base_parser])
snap_create_parser.set_defaults(func=snap_create)

snap_remove_parser = subparsers.add_parser("snap-remove", help="Remove snapshot from a VM",
                                           parents=[vm_base_parser, snap_search_base_parser, snap_arg_base_parser])
snap_remove_parser.set_defaults(func=snap_remove)

revert_parser = subparsers.add_parser("revert", help="Revert VM to snapshot",
                                      parents=[vm_base_parser, snap_search_base_parser])
revert_parser.add_argument("-s", "--start", help="Start machine after revert", action="store_true", default=False)
revert_parser.add_argument("snap", metavar="SNAP", nargs='?', default=None)
revert_parser.set_defaults(func=snap_revert)

snap_remove_parser = subparsers.add_parser("snap-raw-info", help="Show raw snapshot info",
                                           parents=[vm_base_parser, snap_search_base_parser, snap_arg_base_parser])
snap_remove_parser.set_defaults(func=snap_raw_info)


args = parser.parse_args()
try:
    config = read_config_file(args)
    si = esxi_connect(config, args)
    if args.need_args:
        args.func(si, args)
    else:
        args.func(si)
except EctlException as e:
    print("\nError:\n\t{}".format(e))
    exit(1)
