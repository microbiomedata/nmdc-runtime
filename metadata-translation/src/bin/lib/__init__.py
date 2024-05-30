import os

print("**** init: ", os.path.abspath("."))
# print('**** module file: ', os.path.abspath("."))


# import inspect
# inspect.getfile(nmdc)

from importlib.machinery import SourceFileLoader
import importlib

# spec = importlib.util.spec_from_file_location("module.name", "/path/to/file.py")
# print('*** spec: ', spec)
