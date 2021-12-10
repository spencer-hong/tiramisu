from pathlib import Path
import json
from shutil import copy, rmtree
from layer import Layer
from zipfile import ZipFile
import pandas as pd
from utils import generate_hash, lock_files_read_only
from magic import from_file

class Base:
    def __init__(self, baseDir, description=None):
        if (Path(baseDir) / 'base' / 'metadata.json').is_file():
            with open((Path(baseDir) / 'base' / 'metadata.json').as_posix(), 'r') as f:
                metadata = json.load(f)
            self.baseDir = Path(baseDir)
            self.description = metadata['description']
            self.baseFiles = -1
            self.database = self.read_database()
            self.layers = metadata['layers']

        else:
            self.baseDir = Path(baseDir)
            self.description = description
            self.baseFiles = -1
            self.layers = ['base']
            self.database = None

    def prepare_base(self, fileDir):

        # this variable keeps track of the folders that base files are in
        container_ids = {}

        # check that the base folder is not already created
        # every time prepare_base is run, base gets created
        assert (self.baseDir / 'base').exists() == False

        files = get_all_files(fileDir)

        make_folder(self.baseDir, 'base')

        # dictionary to save all digested files and their information
        file_df_saved = {}

        counter = 1

        newID = self.generate_id()

        # first container is always the root container
        file_df_saved[0] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
                                  'fileExtension': '.folder', 'originalName': 'root',
                                  'originalPath': fileDir,
                                  'relativePath': '0.folder',
                                  'parentID': 0, 'hash': ''}
        container_ids['.'] = 0


        newID = self.generate_id()

        # iterate through all the files/folders found in the source directory
        for file in files:
            extension = file.suffix.lower()

            # first create all entries of possible folder hierarchies
            # test/sub_folder/sub_sub_folder/file would be broken down to
            # test, test/sub_folder, test/sub_sub_folder
            for parent in file.relative_to(fileDir).parents:
                if not parent.as_posix() in container_ids:
                    container_ids[parent.as_posix()] = newID
                    newID = self.generate_id()

            # if the file is a directory
            if file.is_dir():

                # if the parent directory doesn't exist yet, add it to the container_ids
                if not file.relative_to(fileDir).as_posix() in container_ids:
                    # this catches the folders that exist at the root level as the
                    # for loop on top only catches the root ('.') folder, not the folder of interest
                    container_ids[file.relative_to(fileDir).as_posix()] = newID
                fileID = str(newID) + '.folder'
                # save it to the list of dictionaries
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + '.folder',
                                          'layer': 'base',
                                          'fileExtension': '.folder', 'originalName': file.name,
                                          'originalPath': file.as_posix(),
                                          'relativePath':  fileID,
                                          'parentID': container_ids[file.relative_to(fileDir).parent.as_posix()],
                                          'hash': ''}


                # create the folder at the base level
                (self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder')).mkdir(
                    parents=True, exist_ok=True)
                # create the folder stand-in file inside the parent folder
                # this .folder file is empty
                with open(self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder') / fileID,
                          'w') as f:
                    pass
            # if the file is not a directory
            else:



                fileID = str(newID) + extension

                copy_files(file, self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder'),
                           fileID)



                extension, newFile = verify_file_type((self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder')) / fileID / fileID)

                lock_files_read_only(newFile)
                # save it in the list of dictionary
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                          'layer': 'base',
                                          'fileExtension': extension, 'originalName': newFile.name,
                                          'originalPath': newFile.as_posix(),
                                          'relativePath': str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder/' + fileID,
                                          'parentID': container_ids[file.relative_to(fileDir).parent.as_posix()],
                                          'hash': generate_hash(newFile.as_posix())}

                # copy the file from the source directory to the base/parent folder



            newID = self.generate_id()

            counter += 1

        # create the first database without unzipping
        self.database = pd.DataFrame.from_dict(file_df_saved, orient='index')

        # we now move onto unzipping and defoldering files inside zip files
        # get all of the zip files we've digested
        # a list of tuples (containerID, file path)
        zipList = list(
            self.database.loc[self.database.fileExtension == '.zip'][['containerID', 'originalPath']].itertuples(
                index=False, name=None))


        # a while loop to continue as long as we continue to find zip files
        while len(zipList) != 0:

            zipID = str(zipList[0][0]) + '.zip'
            make_folder(self.baseDir / 'base', zipID)
            # we store all of the unzipped files temporarily in a /tmp folder
            extractDir = self.baseDir / 'base' / zipID / 'tmp'
            make_folder(self.baseDir / 'base' / zipID, 'tmp')

            if Path('tmp/__MACOSX').is_dir():
                shutil.rmtree('tmp/__MACOSX')

            with ZipFile(zipList[0][1], 'r') as zipRef:

                listOfFileNames = zipRef.namelist()

                for zipFile in listOfFileNames:
                    # __MACOSX is a artifact of unzipping in MacOS
                    if not '__MACOSX' in zipFile:
                        zipRef.extract(zipFile, extractDir)

            # containerIDs are reset for every zip file
            container_ids = {}

            for file in get_all_files(extractDir):
                for parent in file.relative_to(extractDir).parents:
                    if not parent.as_posix() in container_ids:
                        # if we're at the root parent
                        # we assign it to the zipFile ID
                        # because we aren't actually at the root
                        # we're at the root of the Zip File.
                        if parent.as_posix() == '.':
                            container_ids['.'] = zipList[0][0]
                        else:
                            container_ids[parent.as_posix()] = newID
                            newID = self.generate_id()
                extension = file.suffix.lower()

                if file.is_file():
                    extension, newFile = verify_file_type(file)

                    lock_files_read_only(newFile)
                if extension == '.zip':

                    file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                              'layer': 'base',
                                              'fileExtension': extension, 'originalName': newFile.name,
                                              'originalPath': newFile.as_posix(),
                                              'relativePath': (str(newID) + '.zip'),
                                              'parentID': container_ids[
                                                  file.relative_to(extractDir).parent.as_posix()],
                                              'hash': generate_hash(newFile.as_posix())}
                    # add the zip file to the ongoing zipList
                    zipList.append((newID, newFile.as_posix()))

                    # if the zipfile fileID is same as the fileID of the parent folder that contains the zipFile
                    # copy the zip file into the .zip folder
                    if zipList[0][0] == container_ids[file.relative_to(extractDir).parent.as_posix()]:
                        copy_files(newFile, self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.zip'),
                                   str(newID) + extension)


                    # when file's parents is not at the root of the zip file, but some other folder.
                    # copy the file into a .folder folder, not a .zip folder.
                    else:
                        copy_files(newFile, self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.folder'),
                                   str(newID) + extension)

                # if the file is a directory
                elif file.is_dir():
                    if not file.relative_to(extractDir).as_posix() in container_ids:
                        container_ids[file.relative_to(extractDir).as_posix()] = newID
                    temp_id = str(newID) + '.folder'
                    file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + '.folder',
                                              'layer': 'base',
                                              'fileExtension': '.folder', 'originalName': file.name,
                                              'originalPath': file.as_posix(),
                                              'relativePath': temp_id,
                                              'parentID': container_ids[
                                                  file.relative_to(extractDir).parent.as_posix()],
                                              'hash': ''}

                    # again check, if the immediate parent of the folder is at the root of a zip file
                    # if so, save the .folder file in the .zip folder
                    if zipList[0][0] == container_ids[file.relative_to(extractDir).parent.as_posix()]:
                        with open(self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.zip') / temp_id,
                                  'w') as f:
                            pass
                    # if not, then save the .folder file inside the .folder folder
                    else:
                        with open(self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.folder') / temp_id,
                                  'w') as f:
                            pass
                # if the file is not a directory and not a zip file
                else:

                    # if the file is at the root folder, the file must be copied into the .zip file
                    # since the "root" of the file is at the root of the zip file
                    if file.relative_to(extractDir).parent.as_posix() == '.':
                        file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                                  'layer': 'base', 'fileExtension': extension, 'originalName': newFile.name,
                                                  'originalPath': newFile.as_posix(),
                                                  'relativePath': (Path(str(
                                                      container_ids[
                                                          file.relative_to(
                                                              extractDir).parent.as_posix()]) + '.zip') / (
                                                                           str(newID) + extension)).as_posix(),
                                                  'parentID': container_ids[
                                                      file.relative_to(extractDir).parent.as_posix()],
                                                  'hash': generate_hash(file.as_posix())}

                        copy_files(newFile, self.baseDir / 'base' / (str(
                            container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.zip'),
                                   str(newID) + extension)

                    # if not, copy the file to a .folder folder
                    else:
                        file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                                  'layer': 'base','fileExtension': extension, 'originalName': newFile.name,
                                                  'originalPath': newFile.as_posix(),
                                                  'relativePath': (Path(str(
                                                      container_ids[
                                                          file.relative_to(
                                                              extractDir).parent.as_posix()]) + '.folder') / (
                                                                           str(newID) + extension)).as_posix(),
                                                  'parentID': container_ids[
                                                      file.relative_to(extractDir).parent.as_posix()],
                                                  'hash': generate_hash(file.as_posix())}

                        if zipList[0][0] == container_ids[file.relative_to(extractDir).parent.as_posix()]:
                           raise ValueError('this should never be run here if the logic follows')
                        else:
                            copy_files(newFile, self.baseDir / 'base' / (str(
                                container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.folder'),
                                       str(newID) + extension)


                newID = self.generate_id()
                counter += 1

            del zipList[0]

        self.database = pd.DataFrame.from_dict(file_df_saved, orient='index')
        self.dump_database()
        self.update_metadata()

    #this function must be run before prepare_files()
    # pipelineID should be integer
    def set_layer(self, layerName, layerDescription, pipelineID):

        assert (self.baseDir / layerName).exists() == False

        make_folder(self.baseDir, layerName)

        self.layers.append(layerName)
        tempLayer = Layer(layerName, layerDescription, pipelineID, self.baseDir / layerName)
        tempLayer.update_metadata()

        self.update_metadata()

    def set_files(self, layerName):
        ids = {}
        counter = 0
        layerPath = self.baseDir / layerName
        allFiles = get_all_files(layerPath)
        for file in allFiles:
            if not file.is_dir() and file.name != 'metadata.json':
                ids[counter] = {'containerID': int(file.parent.stem), 'filePhase': self.return_file_phase(file.parent.parent.name) + 1,
                                'layer': layerName,
                                'fileExtension': file.suffix, 'originalName': file.name, 'name': file.parent.name,
                                'originalPath': file.parent.as_posix(),
                                'relativePath': file.parent.relative_to(layerPath).as_posix(),
                                'parentID': int(file.parent.parent.stem),
                                'hash': generate_hash(file.as_posix())}

            counter += 1

        self.database = pd.concat([self.database, pd.DataFrame.from_dict(ids, orient='index')], ignore_index=True)

        self.dump_database()

    # file is the file name (with extension)
    def return_file_parent(self, file=None):

        if file != None:
            ID = int(file.split('.')[0])
            return self.database.loc[self.database.containerID == ID].sort_values('filePhase').iloc[-1]['filePhase']
        else:
            return self.database.loc[self.database.containerID == file].sort_values('filePhase').iloc[-1].set_index(
                'name')['parentID'].to_dict()

    def return_file_phase(self, file=None):
        if file != None:

            ID = int(file.split('.')[0])
            return self.database.loc[self.database.containerID == ID].sort_values('filePhase').iloc[-1]['filePhase']
        else:
            return self.database.sort_values('filePhase').groupby('containerID').tail(-1).set_index('name')[
                'filePhases'].to_dict()

    def dump_database(self):
        self.database.to_parquet( self.baseDir / 'database.parquet')

    def read_database(self):
        self.database = pd.read_parquet(self.baseDir/'database.parquet')

        return self.database

    def update_metadata(self):

        metadata = self.baseDir / 'base'/ 'metadata.json'

        content = {'baseDirectory': self.baseDir.as_posix(), 'description': self.description,
                   'layers': [layer for layer in self.layers], 'baseFiles': self.baseFiles}

        with open(metadata.as_posix(), 'w') as f:
            json.dump(content, f)
            f.write('\n')

    def generate_id(self):
        # auto_increment 1
        new_id = self.baseFiles + 1

        self.baseFiles = new_id

        return new_id


    def prepare_folders(self, fileList, layerName):
        for file in fileList:
            make_folder(self.baseDir / layerName, file)



def get_all_files(directory):
    p = Path(directory)
    filtered = [x for x in p.glob("**/*") if (not x.name.startswith('.'))]

    return filtered

def copy_files(file_dir, to_dir, filename, relative_to=None):
    make_folder(to_dir, filename)
    if relative_to:
        file_dir = relative_to / file_dir

    copy(file_dir, to_dir / filename/filename)


def make_folder(file_dir, name):
    new_folder = file_dir / name

    new_folder.mkdir(parents=True)


def remove_folder(directory):
    rmtree(directory)


# filepath must be a Pathlib object
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
        if extension == '':
            if filetype in inverse_key:
                filepath.rename(filepath.parent / (filepath.stem + inverse_key[filetype]))
                # new_name = filepath.parent / (filepath.stem + inverse_key[filetype])

                filepath.parent.rename(filepath.parent.parent / (filepath.stem + inverse_key[filetype]))
                return inverse_key[filetype], filepath.parent.parent / (filepath.stem + inverse_key[filetype]) / (filepath.stem + inverse_key[filetype])
            else:
                print(TypeError(f'Wrong filetype: {filetype} is not in the inverse key.'))
                return extension, filepath

        # elif extension.lower() in built_in_key:
            # filepath.rename(filepath.parent / filepath.stem + inverse_key[filetype])
            # os.rename(filepath, os.path.join(os.path.dirname(filepath), Path(filename).stem + inverse_key[filetype]))
            # new_name = Path(filepath).parent / (Path(filename).stem + inverse_key[filetype])
            # return filetype, new_name

        else:
            print(TypeError(f'Wrong filetype: {extension} is not valid.'))
            return extension, filepath
    else:
        # this is when the file extension is correct
        if built_in_key[extension] == filetype:
            return extension, filepath
        else:
            # if the file isn't binary
            if filetype in inverse_key:
                filepath.rename(filepath.parent / (filepath.stem + inverse_key[filetype]))
                filepath.parent.rename(filepath.parent.parent / (filepath.stem + inverse_key[filetype]))
                # new_name = filepath.parent / (filepath.stem + inverse_key[filetype])
                return inverse_key[filetype], filepath.parent.parent / (filepath.stem + inverse_key[filetype]) / (filepath.stem + inverse_key[filetype])
            else:
                return extension, filepath


built_in_key = {".doc" :  "application/msword",
    ".dot"   :  "application/msword",
    ".docx"  :   "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".dotx"  :   "application/vnd.openxmlformats-officedocument.wordprocessingml.template",
    ".docm"  :   "application/vnd.ms-word.document.macroEnabled.12",
    ".dotm"  :   "application/vnd.ms-word.template.macroEnabled.12",
    ".xls"   : "application/vnd.ms-excel",
    ".xlt"   :  "application/vnd.ms-excel",
    ".xla"   :  "application/vnd.ms-excel",
    ".xlsx"  :   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xltx"  :   "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
    ".xlsm"  :   "application/vnd.ms-excel.sheet.macroEnabled.12",
    ".xltm"  :   "application/vnd.ms-excel.template.macroEnabled.12",
    ".xlam"  :   "application/vnd.ms-excel.addin.macroEnabled.12",
    ".xlsb"  :   "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
    ".ppt"   :"application/vnd.ms-powerpoint",
    ".pot"   :  "application/vnd.ms-powerpoint",
    ".pps"   :  "application/vnd.ms-powerpoint",
    ".ppa"   :  "application/vnd.ms-powerpoint",
    ".pptx"  :   "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".potx"  :   "application/vnd.openxmlformats-officedocument.presentationml.template",
    ".ppsx"  :   "application/vnd.openxmlformats-officedocument.presentationml.slideshow",
    ".ppam"  :   "application/vnd.ms-powerpoint.addin.macroEnabled.12",
    ".pptm"  :   "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
    ".potm"  :   "application/vnd.ms-powerpoint.template.macroEnabled.12",
    ".ppsm"  :   "application/vnd.ms-powerpoint.slideshow.macroEnabled.12",
    ".mdb"   : "application/vnd.ms-access",
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

inverse_key = {"application/msword" : ".doc",
"application/vnd.openxmlformats-officedocument.wordprocessingml.document" : ".docx" ,
"application/vnd.openxmlformats-officedocument.wordprocessingml.template" : ".dotx" ,
"application/vnd.ms-word.document.macroEnabled.12" : ".docm",
"application/vnd.ms-word.template.macroEnabled.12" : ".dotm",
"application/vnd.ms-excel" : ".xls",
"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
"application/vnd.openxmlformats-officedocument.spreadsheetml.template" : ".xltx",
"application/vnd.ms-excel.sheet.macroEnabled.12" : ".xlsm" ,
"application/vnd.ms-excel.template.macroEnabled.12" : ".xltm",
"application/vnd.ms-excel.addin.macroEnabled.12" : ".xlam",
"application/vnd.ms-excel.sheet.binary.macroEnabled.12" : ".xlsb" ,
"application/vnd.ms-powerpoint" : ".ppt",
"application/vnd.openxmlformats-officedocument.presentationml.presentation" : ".pptx",
"application/vnd.openxmlformats-officedocument.presentationml.template" :  ".potx",
"application/vnd.openxmlformats-officedocument.presentationml.slideshow" : ".ppsx",
"application/vnd.ms-powerpoint.addin.macroEnabled.12": ".ppam",
"application/vnd.ms-powerpoint.presentation.macroEnabled.12" : "pptm",
"application/vnd.ms-powerpoint.template.macroEnabled.12" : "potm",
"application/vnd.ms-powerpoint.slideshow.macroEnabled.12": "ppsm",
"application/vnd.ms-access": ".mdb",
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

if __name__ == '__main__':
    fake_folder_with_files = '/Users/spencerhong/Downloads/test_folder/'
    fake_base = '/Users/spencerhong/Downloads/test_base/'

    test_base = Base(fake_base, description='test')

    test_base.prepare_base(fake_folder_with_files)
