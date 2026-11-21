"""Imports the reference as a module so a broken copy can replace one rule."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "reference"))
import reference  # noqa: E402,F401
