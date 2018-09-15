#!/usr/bin/python
# Copyright 2010 Google Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

# Google's Python Class
# http://code.google.com/edu/languages/google-python-class/

import os
import re
import sys
import urllib

"""Logpuzzle exercise
Given an apache logfile, find the puzzle urls and download the images.

Here's what a puzzle url looks like:
10.254.254.28 - - [06/Aug/2007:00:13:48 -0700] "GET /~foo/puzzle-bar-aaab.jpg HTTP/1.0" 302 528 "-" "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.6) Gecko/20070725 Firefox/2.0.0.6"
"""


def read_urls(filename):
  """Returns a list of the puzzle urls from the given log file,
  extracting the hostname from the filename itself.
  Screens out duplicate urls and returns the urls sorted into
  increasing order."""
  # +++your code here+++
  # Open the log file
  try:
    logfile = open(filename, 'rU')
  except IOError:
    print 'Error: File not found (' + filename + ')'
    sys.exit(1)

  # extract out the urls
  prefix = 'http://code.google.com'
  urls = []
  for line in logfile:
    results = re.search('GET (.*puzzle.*) HTTP', line)
    if results:
      urls.append(prefix + results.group(1))

  # Sort and de-duplicate the url list
  urls = sorted(urls, key=lambda url: url.split('/')[-1].split('-')[-1])
  urls = [url for i,url in enumerate(urls) if i == 0 or url != urls[i - 1]]
  return urls



def download_images(img_urls, dest_dir):
  """Given the urls already in the correct order, downloads
  each image into the given directory.
  Gives the images local filenames img0, img1, and so on.
  Creates an index.html in the directory
  with an img tag to show each local image file.
  Creates the directory if necessary.
  """
  # +++your code here+++
  if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)
  for url, i in zip(img_urls, range(0, len(img_urls) - 1)): 
    try:
      urllib.urlretrieve(url, os.path.join(dest_dir, 'img' + str(i)))
    except IOError: 
      print 'Error: could not read from url (' + url + ')'
      sys.exit(1)

  index = open(os.path.join(dest_dir, 'index.html'), 'a')
  index.write('<verbatim>\n<html>\n<body>\n')
  for i in range(0, len(img_urls) - 1):
    index.write('<img src="' + os.path.abspath(os.path.join(dest_dir, 'img' + str(i))) + '">')
  index.write('</body>\n</html>')

def main():
  args = sys.argv[1:]

  if not args:
    print 'usage: [--todir dir] logfile '
    sys.exit(1)

  todir = ''
  if args[0] == '--todir':
    todir = args[1]
    del args[0:2]

  img_urls = read_urls(args[0])

  if todir:
    download_images(img_urls, todir)
  else:
    print '\n'.join(img_urls)

if __name__ == '__main__':
  main()
