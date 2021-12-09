import sys
sys.path.append('../src')
from base import Base

fake_folder_with_files = 'raw_corpus'
fake_base = 'digested_corpus'

test_base = Base(fake_base, description='test')

test_base.prepare_base(fake_folder_with_files)

# we prepare a fake modification to file 13.

test_base.set_layer('layer1', 'first layer test', 1)

test_base.prepare_folders(['14.yaml'], 'layer1')

# here we add the fake modification


# we pick back up the script. but now we no longer have test_base in memory.
# we have to load test_base
#
# test_base = Base(fake_base)
#
# test_base.set_files('layer1')