import hashlib
import os
from stat import S_IREAD, S_IWRITE, S_IEXEC

def generate_hash(filepath):
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536# lets read stuff in 64kb chunks!

    md5 = hashlib.md5()
    # sha1 = hashlib.sha1()

    with open(filepath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
            # sha1.update(data)

    return md5.hexdigest()


def lock_files_read_only(filepath):

    # Replace the first parameter with your file name
    os.chmod(filepath, S_IREAD)

def unlock_files_read_only(filepath):
    os.chmod(filepath, S_IREAD | S_IWRITE | S_IEXEC)


if __name__ == '__main__':
    test_file = '/Users/spencerhong/Documents/GitHub/tiramisu/example/raw_corpus/test.docx'

    lock_files_read_only(test_file)

    unlock_files_read_only(test_file)

