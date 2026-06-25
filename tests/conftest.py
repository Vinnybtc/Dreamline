import sys
from pathlib import Path

# Make dreamline.py (and qr.py) importable from the repo root.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
