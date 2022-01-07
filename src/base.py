from pathlib import Path
import json
from shutil import copy, rmtree
from layer import Layer
from zipfile import ZipFile
import pandas as pd
from utils import generate_hash, lock_files_read_only
from magic import from_file
from uuid import uuid4
import numpy as np 
from random import randrange
from datetime import datetime
from pdb import set_trace as bp
from os.path import getmtime

class Base:
    def __init__(self, baseDir, description=None):
        if (Path(baseDir) / 'base' / 'metadata.json').is_file():
            with open((Path(baseDir) / 'base' / 'metadata.json').as_posix(), 'r') as f:
                metadata = json.load(f)
            self.baseDir = Path(baseDir).resolve()
            self.description = metadata['description']
            self.database = self.read_database()
            self.layers = metadata['layers']

        else:
            self.baseDir = Path(baseDir)
            self.description = description
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
        file_df_saved[0] = {'containerID': newID, 'filePhase': 0, 'name': newID + '.folder', 'layer': 'base',
                                  'fileExtension': '.folder', 'originalName': 'root',
                                  'originalPath': fileDir,
                                  'relativePath': newID + '.folder',
                                  'parentID': newID, 'hash': '', 'time': datetime.now()}
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
                fileID = newID + '.folder'
                # save it to the list of dictionaries
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': newID + '.folder',
                                          'layer': 'base',
                                          'fileExtension': '.folder', 'originalName': file.name,
                                          'originalPath': file.as_posix(),
                                          'relativePath':  fileID,
                                          'parentID': container_ids[file.relative_to(fileDir).parent.as_posix()],
                                          'hash': '',
                                          'time': datetime.now()}


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



                fileID = newID + extension

                copy_files(file, self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder'),
                           fileID)



                extension, newFile = verify_file_type((self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder')) / fileID / fileID)

                lock_files_read_only(newFile)

                # save it in the list of dictionary
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': newID + extension,
                                          'layer': 'base',
                                          'fileExtension': extension, 'originalName': file.name,
                                          'originalPath': file.as_posix(),
                                          'relativePath': str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder/' + fileID,
                                          'parentID': container_ids[file.relative_to(fileDir).parent.as_posix()],
                                          'hash': generate_hash(newFile.as_posix()),
                                          'time': datetime.now()}

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
                                              'relativePath': (newID + '.zip'),
                                              'parentID': container_ids[
                                                  file.relative_to(extractDir).parent.as_posix()],
                                              'hash': generate_hash(newFile.as_posix()),
                                              'time': datetime.now()}
                    # add the zip file to the ongoing zipList
                    zipList.append((newID, newFile.as_posix()))

                    # if the zipfile fileID is same as the fileID of the parent folder that contains the zipFile
                    # copy the zip file into the .zip folder
                    if zipList[0][0] == container_ids[file.relative_to(extractDir).parent.as_posix()]:
                        copy_files(newFile, self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.zip'),
                                   newID + extension)


                    # when file's parents is not at the root of the zip file, but some other folder.
                    # copy the file into a .folder folder, not a .zip folder.
                    else:
                        copy_files(newFile, self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.folder'),
                                   newID + extension)

                # if the file is a directory
                elif file.is_dir():
                    if not file.relative_to(extractDir).as_posix() in container_ids:
                        container_ids[file.relative_to(extractDir).as_posix()] = newID
                    temp_id = newID + '.folder'
                    file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + '.folder',
                                              'layer': 'base',
                                              'fileExtension': '.folder', 'originalName': file.name,
                                              'originalPath': file.as_posix(),
                                              'relativePath': temp_id,
                                              'parentID': container_ids[
                                                  file.relative_to(extractDir).parent.as_posix()],
                                              'hash': '',
                                              'time': datetime.now()}

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
                                                  'hash': generate_hash(file.as_posix()),
                                                  'time': datetime.now()}

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
                                                  'hash': generate_hash(file.as_posix()),
                                                  'time': datetime.now()}

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
    def set_layer(self, layerName, layerDescription, pipelineID, fileList):

        assert (self.baseDir / layerName).exists() == False
        assert not (layerName in self.layers )

        make_folder(self.baseDir, layerName)

        self.layers.append(layerName)
        tempLayer = Layer(layerName, layerDescription, pipelineID, self.baseDir / layerName)
        tempLayer.update_metadata()

        self.prepare_folders(fileList, layerName)

        self.update_metadata()

    def set_files(self, layerName):
        ids = {}
        counter = 0
        layerPath = self.baseDir / layerName
        allFiles = get_all_files(layerPath)
        for file in allFiles:

            if not file.is_dir() and not (file.name  in ['metadata.json', 'database.parquet']):
                ids[counter] = {'containerID': file.parent.stem, 'filePhase': self.return_file_phase(file.parent.parent.name) + 1,
                                'layer': layerName,
                                'fileExtension': file.suffix, 'originalName': file.name, 'name': file.parent.name,
                                'originalPath': file.parent.as_posix(),
                                'relativePath': file.parent.relative_to(layerPath).as_posix(),
                                'parentID': file.parent.parent.stem,
                                'hash': generate_hash(file.as_posix()),
                                'time': getmtime(file)}

            counter += 1
        self.database = pd.concat([self.database, pd.DataFrame.from_dict(ids, orient='index')], ignore_index=True)

        self.dump_database()

    def load_layer(self, layerName):

        with open(self.baseDir/ layerName/'metadata.json', 'r') as f:
            metadata = json.load(f)
        return Layer(layerName, metadata['description'], metadata['pipelineID'], metadata['layerDirectory'])

    # file is the file name (with extension)
    # this returns the immediate parent, not all parents.
    def return_file_parent(self, file=None):
        if file != None:
            ID = file.split('.')[0]

            return self.database.loc[self.database.containerID == ID].sort_values('filePhase').iloc[-1]['filePhase']
        else:
            return self.database.loc[self.database.containerID == file].sort_values('filePhase').iloc[-1].set_index(
                'name')['parentID'].to_dict()

    def return_file_phase(self, file=None):
        if file != None:

            ID = file.split('.')[0]
            return self.database.loc[self.database.containerID == ID].sort_values('filePhase').iloc[-1]['filePhase']
        else:
            return self.database.sort_values('filePhase').groupby('containerID').tail(-1).set_index('name')[
                'filePhases'].to_dict()

    def dump_database(self):
        self.database.containerID = self.database.containerID.astype(str)
        self.database.parentID = self.database.parentID.astype(str)

        self.database.to_parquet( self.baseDir / 'database.parquet')
    def read_database(self):
        self.database = pd.read_parquet(self.baseDir/'database.parquet')

        return self.database

    def update_metadata(self):

        metadata = self.baseDir / 'base'/ 'metadata.json'

        content = {'baseDirectory': self.baseDir.as_posix(), 'description': self.description,
                   'layers': [layer for layer in self.layers]}

        with open(metadata.as_posix(), 'w') as f:
            json.dump(content, f)
            f.write('\n')

    def generate_id(self):
        # auto_increment 1
        # new_id = self.baseFiles + 1

        # self.baseFiles = new_id

        # return new_id
 
        return uuid4().hex + str(randrange(100000))



    def prepare_folders(self, fileList, layerName):
        for file in fileList:
            make_folder(self.baseDir / layerName, file)

    def return_parent_child_table(self):

        def list_ancestors(edges):
            """
            Take edge list of a rooted tree as a numpy array with shape (E, 2),
            child nodes in edges[:, 0], parent nodes in edges[:, 1]
            Return pandas dataframe of all descendant/ancestor node pairs

            Ex:
                df = pd.DataFrame({'child': [200, 201, 300, 301, 302, 400],
                                   'parent': [100, 100, 200, 200, 201, 300]})

                df
                   child  parent
                0    200     100
                1    201     100
                2    300     200
                3    301     200
                4    302     201
                5    400     300

                list_ancestors(df.values)

                returns

                    descendant  ancestor
                0          200       100
                1          201       100
                2          300       200
                3          300       100
                4          301       200
                5          301       100
                6          302       201
                7          302       100
                8          400       300
                9          400       200
                10         400       100
            """
            ancestors = []
            for ar in trace_nodes(edges):
                ancestors.append(np.c_[np.repeat(ar[:, 0], ar.shape[1] - 1),
                                       ar[:, 1:].flatten()])
            return pd.DataFrame(np.concatenate(ancestors),
                                columns=['child', 'parent'])

        def trace_nodes(edges):
            """
            Take edge list of a rooted tree as a numpy array with shape (E, 2),
            child nodes in edges[:, 0], parent nodes in edges[:, 1]
            Yield numpy array with cross-section of tree and associated
            ancestor nodes

            Ex:
                df = pd.DataFrame({'child': [200, 201, 300, 301, 302, 400],
                                   'parent': [100, 100, 200, 200, 201, 300]})

                df
                   child  parent
                0    200     100
                1    201     100
                2    300     200
                3    301     200
                4    302     201
                5    400     300

                trace_nodes(df.values)

                yields

                array([[200, 100],
                       [201, 100]])

                array([[300, 200, 100],
                       [301, 200, 100],
                       [302, 201, 100]])

                array([[400, 300, 200, 100]])
            """
            mask = np.in1d(edges[:, 1], edges[:, 0])
            gen_branches = edges[~mask]
            edges = edges[mask]
            yield gen_branches
            while edges.size != 0:
                mask = np.in1d(edges[:, 1], edges[:, 0])
                next_gen = edges[~mask]
                gen_branches = numpy_col_inner_many_to_one_join(next_gen, gen_branches)
                edges = edges[mask]
                yield gen_branches

        def numpy_col_inner_many_to_one_join(ar1, ar2):
            """
            Take two 2-d numpy arrays ar1 and ar2,
            with no duplicate values in first column of ar2
            Return inner join of ar1 and ar2 on
            last column of ar1, first column of ar2

            Ex:

                ar1 = np.array([[1,  2,  3],
                                [4,  5,  3],
                                [6,  7,  8],
                                [9, 10, 11]])

                ar2 = np.array([[ 1,  2],
                                [ 3,  4],
                                [ 5,  6],
                                [ 7,  8],
                                [ 9, 10],
                                [11, 12]])

                numpy_col_inner_many_to_one_join(ar1, ar2)

                returns

                array([[ 1,  2,  3,  4],
                       [ 4,  5,  3,  4],
                       [ 9, 10, 11, 12]])
            """

            ar1 = ar1[np.in1d(ar1[:, -1], ar2[:, 0])]
            ar2 = ar2[np.in1d(ar2[:, 0], ar1[:, -1])]

            if 'int' in ar1.dtype.name and ar1[:, -1].min() >= 0:
                bins = np.bincount(ar1[:, -1])
                counts = bins[bins.nonzero()[0]]
            else:
                counts = np.unique(ar1[:, -1], False, False, True)[1]
            left = ar1[ar1[:, -1].argsort()]
            right = ar2[ar2[:, 0].argsort()]
            return np.concatenate([left[:, :-1],
                                   right[np.repeat(np.arange(right.shape[0]),
                                                   counts)]], 1)
        return list_ancestors(self.database[['containerID', 'parentID']].iloc[1:].values)



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
                print(TypeError(f'Wrong filetype: {filetype} is not in the inverse key. \nReturning {filename} as is.'))
                return extension, filepath
        else:
            print(TypeError(f'Wrong filetype: {extension} is not in our template. \nReturning {filename} as is.'))
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
