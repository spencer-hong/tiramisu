from pathlib import Path
import json
from shutil import copy, rmtree
from layer import Layer
from zipfile import ZipFile
import pandas as pd


class Base:
    def __init__(self, baseDir, description=None):
        self.baseDir = Path(baseDir)
        self.description = description
        self.baseFiles = []
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
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
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
                file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
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
                    file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
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
                    file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
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
                        file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
                                                  'fileExtension': extension, 'originalName': file.name,
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
                        file_df_saved[counter] = {'containerID': newID, 'filePhase': 0, 'layer': 'base',
                                                  'fileExtension': extension, 'originalName': file.name,
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

    def set_layer(self, layer_name, layer_description, pipeline_id):

        assert (self.baseDir / layer_name).exists() == False

        make_folder(self.baseDir, layer_name)

        self.layers.append(layer_name)
        temp_layer = Layer(layer_name, layer_description, pipeline_id, self.baseDir / layer_name)
        temp_layer.update_metadata()

    def set_files(self, layer_name):

        ids = {}
        counter = 0
        layer_path = self.baseDir / layer_name
        files = get_all_files(layer_path)

        for file in files:
            # for parent in file.relative_to(layer_path).parents:
            # 	if parent.as_posix() != '.':
            if file.suffix == '.folder':
                ids[counter] = {'containerID': int(file.stem), 'filePhase': self.layers.index(layer_name),
                                'layer': layer_name,
                                'fileExtension': '.folder', 'originalName': file.name,
                                'originalPath': file.as_posix(),
                                'relativePath': file.relative_to(layer_path).as_posix(),
                                'parentID': file.parent.name}
            else:
                if not file.is_dir():
                    ids[counter] = {'containerID': int(file.parent.stem), 'filePhase': layer_name,
                                    'fileExtension': file.parent.suffix, 'originalName': file.name,
                                    'originalPath': file.as_posix(),
                                    'relativePath': file.relative_to(layer_path).as_posix(),
                                    'parentID': return_file_parent(file=int(file.stem))}

            counter += 1

        self.database = pd.concat([self.database, pd.DataFrame.from_dict(ids, orient='index')], ignore_index=True)

    def return_file_parent(self, file=None):

        if file != None:
            return self.database.loc[self.database.containerID == file].sort_values('filePhase').iloc[-1].set_index(
                'containerID')['parentID'].to_dict()[file]
        else:
            return self.database.loc[self.database.containerID == file].sort_values('filePhase').iloc[-1].set_index(
                'containerID')['parentID'].to_dict()

    def return_file_phase(self, file=None):

        if file != None:
            return self.database.loc[self.database.containerID == file].sort_values('filePhase').iloc[-1].set_index(
                'containerID')['filePhases'].to_dict()
        else:
            return self.database.sort_values('filePhase').groupby('containerID').tail(-1).set_index('containerID')[
                'filePhases'].to_dict()

    def dump_database(self):
        self.database.to_parquet( self.baseDir / 'database.parquet')

    def read_database(self):
        self.database = pd.read_parquet(self.baseDir/'database.parquet')

    def update_metadata(self):

        metadata = self.base_dir / 'metadata.json'

        content = {'baseDirectory': self.base_dir.as_posix(), 'description': self.description,
                   'layers': [layer.layer_name for layer in self.layers]}

        with open(metadata.as_posix(), 'w') as f:
            json.dump(content, f)
            f.write('\n')

    def generate_id(self):
        # auto_increment 1

        if len(self.baseFiles) == 0:
            self.baseFiles.append(0)
            return 0
        else:
            new_id = sorted(self.baseFiles)[-1] + 1

            self.baseFiles.append(new_id)
            return new_id

    def read(self):

        with open(self.baseDir / 'metadata.json', 'r') as f:
            metadata = [json.loads(line) for line in f]

        self.description = metadata[0]['description']

        self.layers = []
        layers = metadata[0]['layers']

        for layer in layers:
            layer_path = self.baseDir / layer

            with open(layer_path / 'metadata.json', 'r') as f:
                layer_data = json.load(f)

            self.layers.append(Layer(layer_data['layer_name'], layer_data['description'], layer_data['pipelineID'],
                                     layer_data['layerDirectory']))

        self.baseFiles = [int(x) for x in metadata[1].keys()]

        self.filephases = metadata[1]


def get_all_files(directory):
    p = Path(directory)
    filtered = [x for x in p.glob("**/*") if (not x.name.startswith('.'))]

    return filtered


def copy_files(file_dir, to_dir, filename, relative_to=None):
    id_name = filename.strip('f').split('.')[0]
    make_folder(to_dir, filename)
    if relative_to:
        file_dir = relative_to / file_dir
    copy(file_dir, to_dir / filename)


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
