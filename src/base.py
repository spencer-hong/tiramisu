from pathlib import Path
import json
from shutil import copy, rmtree
from layer import Layer
from zipfile import ZipFile
import pandas as pd
# from magic import from_file

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
                                  'parentID': 0}
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
                                          'parentID': container_ids[file.relative_to(fileDir).parent.as_posix()]}




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
                # save it in the list of dictionary
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                          'layer': 'base',
                                          'fileExtension': extension, 'originalName': file.name,
                                          'originalPath': file.as_posix(),
                                          'relativePath': str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder/' + fileID,
                                          'parentID': container_ids[file.relative_to(fileDir).parent.as_posix()]}

                # copy the file from the source directory to the base/parent folder
                copy_files(file, self.baseDir / 'base' / (
                        str(container_ids[file.relative_to(fileDir).parent.as_posix()]) + '.folder'),
                           fileID)


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

                if extension == '.zip':
                    file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                              'layer': 'base',
                                              'fileExtension': extension, 'originalName': file.name,
                                              'originalPath': file.as_posix(),
                                              'relativePath': (str(newID) + '.zip'),
                                              'parentID': container_ids[
                                                  file.relative_to(extractDir).parent.as_posix()]}
                    # add the zip file to the ongoing zipList
                    zipList.append((newID, file.as_posix()))

                    # if the zipfile fileID is same as the fileID of the parent folder that contains the zipFile
                    # copy the zip file into the .zip folder
                    if zipList[0][0] == container_ids[file.relative_to(extractDir).parent.as_posix()]:
                        copy_files(file, self.baseDir / 'base' / (
                                str(container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.zip'),
                                   str(newID) + extension)

                    # when file's parents is not at the root of the zip file, but some other folder.
                    # copy the file into a .folder folder, not a .zip folder.
                    else:
                        copy_files(file, self.baseDir / 'base' / (
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
                                                  file.relative_to(extractDir).parent.as_posix()]}

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
                                                  'layer': 'base', 'fileExtension': extension, 'originalName': file.name,
                                                  'originalPath': file.as_posix(),
                                                  'relativePath': (Path(str(
                                                      container_ids[
                                                          file.relative_to(
                                                              extractDir).parent.as_posix()]) + '.zip') / (
                                                                           str(newID) + extension)).as_posix(),
                                                  'parentID': container_ids[
                                                      file.relative_to(extractDir).parent.as_posix()]}
                        copy_files(file, self.baseDir / 'base' / (str(
                            container_ids[file.relative_to(extractDir).parent.as_posix()]) + '.zip'),
                                   str(newID) + extension)

                    # if not, copy the file to a .folder folder
                    else:
                        file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'name': str(newID) + extension,
                                                  'layer': 'base','fileExtension': extension, 'originalName': file.name,
                                                  'originalPath': file.as_posix(),
                                                  'relativePath': (Path(str(
                                                      container_ids[
                                                          file.relative_to(
                                                              extractDir).parent.as_posix()]) + '.folder') / (
                                                                           str(newID) + extension)).as_posix(),
                                                  'parentID': container_ids[
                                                      file.relative_to(extractDir).parent.as_posix()]}

                        if zipList[0][0] == container_ids[file.relative_to(extractDir).parent.as_posix()]:
                           raise ValueError('this should never be run here if the logic follows')
                        else:
                            copy_files(file, self.baseDir / 'base' / (str(
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

    # requires the two dictionaries from prepare_files()
    def set_files(self, layerName):
        # self.baseFiles = self.database.sort_values('containerID').iloc[-1]['containerID'] + 1
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
                                'parentID': int(file.parent.parent.stem)}
            # children = get_all_files(layerPath / file)
            # # for parent in file.relative_to(layer_path).parents:
            # # 	if parent.as_posix() != '.':
            #
            # for child in children:
            #     if child.is_dir():
            #         subChildren = get_all_files(child)
            #         for subChild in subChildren:
            #             ids[counter] = {'containerID': int(child.stem), 'filePhase': filephaseDict[file],
            #                     'layer': layerName,
            #                     'fileExtension': child.suffix, 'originalName': subChild.name,'name': child.name,
            #                     'originalPath': subChild.as_posix(),
            #                     'relativePath': subChild.relative_to(layerPath).as_posix(),
            #                     'parentID': int(file.split('.')[0])}

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
        # if len(self.baseFiles) == 0:
        #     self.baseFiles.append(0)
        #     return 0
        # else:
        #     new_id = sorted(self.baseFiles)[-1] + 1
        #
        #     self.baseFiles.append(new_id)
        #     return new_id

    def prepare_folders(self, fileList, layerName):
        for file in fileList:
            make_folder(self.baseDir / layerName, file)
    # def prepare_files(self, fileList, layerName):
    #
    #     filePhaseDict = {}
    #     idDict ={}
    #     for file in fileList:
    #         filePhase = self.return_file_phase(file)
    #         newFilePhase = filePhase + 1
    #
    #         newID = self.generate_id()
    #
    #         filePhaseDict[file] = newFilePhase
    #         idDict[file] = newID
    #
    #     return filePhaseDict, idDict


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


if __name__ == '__main__':
    fake_folder_with_files = '/Users/spencerhong/Downloads/test_folder/'
    fake_base = '/Users/spencerhong/Downloads/test_base/'

    test_base = Base(fake_base, description='test')

    test_base.prepare_base(fake_folder_with_files)
