#!/usr/bin/python
# Copyright 2010 Google Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

# Google's Python Class
# http://code.google.com/edu/languages/google-python-class/

import sys
import re
import os
import shutil
import commands

"""Copy Special exercise
"""

# +++your code here+++
# Write functions and modify main() to call them
def get_special_paths(dir):
  try:
    f_descriptors = os.listdir(dir)
  except IOError:
    handle_io_exception()

  special_files = []
  for f_desc in f_descriptors:
    if re.search('.*__[a-zA-Z]+__.*', f_desc):
      special_files.append(os.path.abspath(f_desc))
  return special_files


def copy_to(files, to_dir):
  # Let's make the directory first if we need to
  if not os.path.exists(to_dir):
    os.mkdir(to_dir)
  for f in files:
    shutil.copy(f, to_dir)


def zip_to(paths, zippath):
  if os.path.exists(zippath):
    print "Archive already exists, please try again."
    sys.exit(1)
  
  files_to_archive = ''
  for f_desc in paths:
    files_to_archive = files_to_archive + f_desc + ' '
    
  (status, output) = commands.getstatusoutput('zip -j ' + zippath + ' ' + files_to_archive)
  if status:
    print output
    print 'zip error: Could not create output file (/no/way.zip)'
    sys.exit(status)

def handle_io_exception():
  print 'File or directory not found'
  sys.exit(1)

def main():
  # This basic command line argument parsing code is provided.
  # Add code to call your functions below.

  # Make a list of command line arguments, omitting the [0] element
  # which is the script itself.
  args = sys.argv[1:]
  if not args:
    print "usage: [--todir dir][--tozip zipfile] dir [dir ...]";
    sys.exit(1)

  # todir and tozip are either set from command line
  # or left as the empty string.
  # The args array is left just containing the dirs.
  todir = ''
  if args[0] == '--todir':
    todir = args[1]
    del args[0:2]

  tozip = ''
  if args[0] == '--tozip':
    tozip = args[1]
    del args[0:2]

  if len(args) == 0:
    print "error: must specify one or more dirs"
    sys.exit(1)

  # +++your code here+++
  # Call your functions
  if todir:
    special_files = []
    for dir in args:
      for special_file in get_special_paths(dir):
        special_files.append(special_file)
    copy_to(special_files, todir)
    sys.exit(0)

  if tozip:
    special_files = []
    for dir in args:
      for special_file in get_special_paths(dir):
        special_files.append(special_file)
    zip_to(special_files, tozip)
    sys.exit(0)
      
  for dir in args:
    for f_desc in get_special_paths(dir):
      print f_desc


  
if __name__ == "__main__":
    main()
