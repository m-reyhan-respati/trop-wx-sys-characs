from pathlib import Path
import os

username = os.environ["USER"]
ROOT_DIR = os.environ["ROOT"]

ISCCP_OLR_DIR = "/g/data/k10/mr4682/data/ISCCP/txuptp"
ERA5_RT52_DIR = "/g/data/rt52/era5"
GPM_IA39_DIR = "/g/data/ia39/aus-ref-clim-data-nci/gpm/data/V07"

SCRATCH_ERA5_DIR = "/scratch/k10/mr4682/data/ERA5"
SCRATCH_GPM_DIR = "/scratch/k10/mr4682/data/GPM"
SCRATCH_ISCCP_DIR = "/scratch/k10/mr4682/data/ISCCP"
SCRATCH_TRACK_DIR = "/scratch/k10/mr4682/data/track_isccp_olr"

IBTrACS_FILE = "/g/data/k10/mr4682/data/IBTrACS/IBTrACS.ALL.v04r00.nc"
