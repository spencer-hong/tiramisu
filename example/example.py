import sys
sys.path.append('../src')
from base import Base

fake_folder_with_files = 'raw_corpus'
fake_base = 'digested_corpus'

test_base = Base(fake_base, description='test')

test_base.prepare_base(fake_folder_with_files)