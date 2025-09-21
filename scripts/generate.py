import os
import sys

from identity.generation import generate_bulk


while True:
    for id in generate_bulk(10):
        sys.stdout.write('%s\n' % id)
