import os
import zlib
from stat import S_IREAD, S_IWRITE, S_IEXEC
from shutil import copy, rmtree
from pathlib import Path
from pdb import set_trace as bp
from magic import from_file
import hashlib

# partially taken from @holger, treelib repository


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


def crc32(data):
    data = bytes(data, 'UTF-8')

    return hex(zlib.crc32(data) & 0xffffffff)  # crc32 returns a signed value, &-ing it will match py3k

def assign_node_id(depth, root, directory):
    # directory has to be a full directory

    return str(str(depth) + '_' + directory).replace(" ", "_") + '+++' + crc32(os.path.join(root, directory))

def get_parent_id(depth, root, directory):
    if depth == 0:
        return 'ROOT'

    # looking for parent directory
    # e.g. /home/user1/mp3/folder1/parent_folder/current_folder
    # get 'parent_folder'

    search_string = os.path.join(root, directory)
    pos2 = search_string.rfind('/')
    pos1 = search_string.rfind('/', 0, pos2)
    parent_dir = search_string[pos1 + 1:pos2]

    parentid = str(depth - 1) + '_' + parent_dir.replace(" ", "_") + '+++' + crc32(root)
    
    return parentid

def copy_files(source_path, to_path):
    copy(source_path, to_path)
    
def write_gitignore(path):
    copy_files('_gitignore_template', Path(path) / '.gitignore')

# removing a non-empty folder
def remove_folder(path):
    rmtree(path)

def lock_files_read_only(filepath):

    # Replace the first parameter with your file name
    os.chmod(filepath, S_IREAD)

def unlock_files_read_only(filepath):
    os.chmod(filepath, S_IREAD | S_IWRITE | S_IEXEC)

def verify_file_type(filepath):
    filename = filepath.name


    if len(filename.split('.')) == 1:
        extension = ''
    else:
        # assert len(filename.split('.')) == 2
        extension = '.' + filename.split('.')[-1]
        extension = extension.lower()
    filetype = from_file(filepath.as_posix(), mime=True)
    new_name = None

    if extension not in built_in_key:
        if filetype == 'application/octet-stream':
            print(TypeError(f'Binary filetype: {filetype} will not be analyzed due to security reasons. \nReturning {filename} as is.'))
            return filepath
        elif extension == '':
            if filetype in inverse_key:
                # filepath.rename(filepath.parent / (filepath.stem + inverse_key[filetype]))

                correctedPath = filepath.parent / (filename + '_tiramisu_corrected')

                correctedPath.mkdir(parents = True, exist_ok = True)

                return correctedPath / (filepath.stem + inverse_key[filetype])
            else:
                print(TypeError(f'Wrong filetype: {filetype} is not in the inverse key. \nReturning {filename} as is.'))
                return filepath
        else:
            print(TypeError(f'Wrong filetype: {extension} is not in our template. \nReturning {filename} as is.'))
            return filepath
    else:
        # this is when the file extension is correct

        # ignore binary files
        if filetype == 'application/octet-stream':
            print(TypeError(f'Binary filetype: {filetype} will not be analyzed due to security reasons. \nReturning {filename} as is.'))

            return filepath
        elif built_in_key[extension] == filetype:
            return filepath
        else:
            # if the file isn't binary
            if filetype in inverse_key:

                # filepath.rename(filepath.parent / (filepath.stem + inverse_key[filetype]))

                correctedPath = filepath.parent / (filename + '_tiramisu_corrected')

                correctedPath.mkdir(parents = True, exist_ok = True)

                return correctedPath / (filepath.stem + inverse_key[filetype])
                
            else:
                print(TypeError(f'Wrong filetype: {extension} is not in our template. \nReturning {filename} as is.'))
                return filepath


built_in_key = {
    ".doc" :  "application/msword",
    ".dot"   :  "application/msword",
    ".docx"  :   "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls"   : "application/vnd.ms-excel",
    ".xlt"   :  "application/vnd.ms-excel",
    ".xlsx"  :   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt"   :"application/vnd.ms-powerpoint",
    ".pptx"  :   "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".csv" :   "text/csv",
    ".gz": "application/gzip",
    ".gif": "image/gif",
    ".html": "text/html",
    ".htm" : "text/html",
    ".jpeg": "image/jpeg",
    ".jpg" : "image/jpg",
    ".json": "application/json",
    ".mp4": "video/mp4",
    ".mpeg": "video/mpeg",
    ".mp3": "audio/mpeg",
    ".png": "image/png",
    ".pdf": "application/pdf",
    ".rar": "application/vnd.rar",
    ".rtf": "application/rtf",
    ".svg": "image/svg+xml",
    ".tar": "application/x-tar",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".txt": "text/plain",
    ".webp": "image/webp",
    ".xml": "application/xml",
    ".zip": "application/zip",
    ".7z": "application/x-7z-compressed",
    ".tar.xz": "application/x-xz",
    ".gzip" : "application/gzip"
}

inverse_key = {
    "application/msword" : ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document" : ".docx" ,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.template" : ".dotx" ,
    "application/vnd.ms-excel" : ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-powerpoint" : ".ppt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation" : ".pptx",
    "text/csv" : ".csv",
    "application/gzip" : ".gz",
    "image/gif" : ".gif",
    "text/html" : ".html",
    "image/jpeg" : '.jpeg',
    "application/json" : '.json',
    "video/mp4" : ".mp4",
    "video/mpeg" : ".mpeg",
    "audio/mpeg" : ".mp3",
    "image/png" : ".png",
    "application/pdf" : ".pdf",
    "application/vnd.rar" : ".rar",
    "application/rtf" : ".rtf",
    "image/svg+xml" : ".svg",
    "application/x-tar" : ".tar",
    "image/tiff" : ".tiff",
    "text/plain" : ".txt",
    "text/rtf": ".rtf",
    "image/webp" : ".webp",
    "application/xml" : ".xml",
    "application/zip" : ".zip",
    "application/x-7z-compressed" : ".7z",
    "application/x-xz" : ".tar.xz"
}
