"""ทำให้ module ที่อยู่ root (config, transform, load, ...) import ได้จาก tests/."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
