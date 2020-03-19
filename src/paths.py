from pathlib import Path
home = Path().home()

import sys

sys.path = sys.path + [str(home) + '/.conda/envs/condaenv/lib/python37.zip', 
                       str(home) + '/.conda/envs/condaenv/lib/python3.7', 
                       str(home) + '/.conda/envs/condaenv/lib/python3.7/lib-dynload', 
                       str(home) + '/.conda/envs/condaenv/lib/python3.7/site-packages']
sys.prefix = str(home / '.conda/envs/condaenv/')

sys.path.insert(0, str(Path(__file__).cwd()))
sys.path.insert(0, '../scripts')
sys.path.insert(0, '../.env/lib/python3.7/site-packages')
sys.path.insert(0, '../src')
