#!/usr/bin/env python3

import argparse
from subprocess import Popen
from time import sleep

__progname__ = "process-restarter"

parser = argparse.ArgumentParser(prog=__progname__)
parser.add_argument('-e', '--exec', action='store', required=True, help='executable of process to restart')

args = parser.parse_args()

exefile = args.exec

sleep(1)
Popen([exefile])
exit(0)
