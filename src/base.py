from pathlib import Path
import json
from shutil import copy, rmtree
from layer import Layer
from zipfile import ZipFile
import pandas as pd


class Base:
    def __init__(self, base_dir, description=None):
        self.base_dir = Path(base_dir)
        self.description = description
        self.base_files = []
        self.layers = ['base']
        self.database = None

    def prepare_base(self, file_dir):
        container_ids = {}
        assert (self.base_dir / 'base').exists() == False

        files = get_all_files(file_dir)

        make_folder(self.base_dir, 'base')

        file_df_saved = {}
        counter = 1
        new_id = self.generate_id()
        file_df_saved[0] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                  'fileExtension': '.folder', 'originalName': 'root',
                                  'originalPath': file_dir,
                                  'relativePath': '0.folder',
                                  'parentID': 0}
        container_ids['.'] = 0
        new_id = self.generate_id()
        for file in files:
            extension = file.suffix.lower()
            for parent in file.relative_to(file_dir).parents:
                if not parent.as_posix() in container_ids:
                    container_ids[parent.as_posix()] = new_id
                    new_id = self.generate_id()
            if file.is_dir():
                if not file.relative_to(file_dir).as_posix() in container_ids:
                    container_ids[file.relative_to(file_dir).as_posix()] = new_id

                file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                          'fileExtension': '.folder', 'originalName': file.name,
                                          'originalPath': file.as_posix(),
                                          'relativePath':  str(new_id) + '.folder',
                                          'parentID': container_ids[file.relative_to(file_dir).parent.as_posix()]}

                temp_id = str(new_id) + '.folder'
                (self.base_dir / 'base' / (
                        str(container_ids[file.relative_to(file_dir).parent.as_posix()]) + '.folder')).mkdir(
                    parents=True, exist_ok=True)

                with open(self.base_dir / 'base' / (
                        str(container_ids[file.relative_to(file_dir).parent.as_posix()]) + '.folder') / temp_id,
                          'w') as f:
                    pass

            else:

                file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                          'fileExtension': extension, 'originalName': file.name,
                                          'originalPath': file.as_posix(),
                                          'relativePath': str(container_ids[file.relative_to(file_dir).parent.as_posix()]) + '.folder/' + str(new_id) + extension,
                                          'parentID': container_ids[file.relative_to(file_dir).parent.as_posix()]}

                copy_files(file, self.base_dir / 'base' / (
                        str(container_ids[file.relative_to(file_dir).parent.as_posix()]) + '.folder'),
                           str(new_id) + extension)

            new_id = self.generate_id()

            counter += 1
        self.database = pd.DataFrame.from_dict(file_df_saved, orient='index')

        zip_list = list(
            self.database.loc[self.database.fileExtension == '.zip'][['containerID', 'originalPath']].itertuples(
                index=False, name=None))

        while len(zip_list) != 0:
            zip_id = str(zip_list[0][0]) + '.zip'
            make_folder(self.base_dir / 'base', zip_id)
            extract_dir = self.base_dir / 'base' / zip_id / 'tmp'
            make_folder(self.base_dir / 'base' / zip_id, 'tmp')
            with ZipFile(zip_list[0][1], 'r') as zip_ref:

                listOfFileNames = zip_ref.namelist()

                for zipFile in listOfFileNames:
                    if not '__MACOSX' in zipFile:
                        zip_ref.extract(zipFile, extract_dir)
            container_ids = {}
            for file in get_all_files(extract_dir):
                for parent in file.relative_to(extract_dir).parents:
                    if not parent.as_posix() in container_ids:
                        if parent.as_posix() == '.':
                            container_ids['.'] = zip_list[0][0]
                        else:
                            container_ids[parent.as_posix()] = new_id
                            new_id = self.generate_id()
                extension = file.suffix.lower()
                if extension == '.zip':
                    file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                              'fileExtension': extension, 'originalName': file.name,
                                              'originalPath': file.as_posix(),
                                              'relativePath': (str(new_id) + '.zip'),
                                              'parentID': container_ids[
                                                  file.relative_to(extract_dir).parent.as_posix()]}

                    zip_list.append((new_id, file.as_posix()))
                elif file.is_dir():
                    if not file.relative_to(extract_dir).as_posix() in container_ids:
                        container_ids[file.relative_to(extract_dir).as_posix()] = new_id

                    file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                              'fileExtension': '.folder', 'originalName': file.name,
                                              'originalPath': file.as_posix(),
                                              'relativePath': str(container_ids[file.relative_to(
                                                  extract_dir).parent.as_posix()]) + '.folder',
                                              'parentID': container_ids[
                                                  file.relative_to(extract_dir).parent.as_posix()]}

                    temp_id = str(new_id) + '.folder'
                    (self.base_dir / 'base' / (
                            str(container_ids[file.relative_to(extract_dir).parent.as_posix()]) + '.folder')).mkdir(
                        parents=True, exist_ok=True)

                    with open(self.base_dir / 'base' / (
                            str(container_ids[file.relative_to(extract_dir).parent.as_posix()]) + '.folder') / temp_id,
                              'w') as f:
                        pass
                else:
                    file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                              'fileExtension': extension, 'originalName': file.name,
                                              'originalPath': file.as_posix(),
                                              'relativePath': (Path(str(
                                                  container_ids[
                                                      file.relative_to(extract_dir).parent.as_posix()]) + '.folder') / (
                                                                       str(new_id) + extension)).as_posix(),
                                              'parentID': container_ids[
                                                  file.relative_to(extract_dir).parent.as_posix()]}


                    if file.relative_to(extract_dir).parent.as_posix() == '.':
                        file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                                  'fileExtension': extension, 'originalName': file.name,
                                                  'originalPath': file.as_posix(),
                                                  'relativePath': (Path(str(
                                                      container_ids[
                                                          file.relative_to(
                                                              extract_dir).parent.as_posix()]) + '.zip') / (
                                                                           str(new_id) + extension)).as_posix(),
                                                  'parentID': container_ids[
                                                      file.relative_to(extract_dir).parent.as_posix()]}
                        copy_files(file, self.base_dir / 'base' / (str(
                            container_ids[file.relative_to(extract_dir).parent.as_posix()]) + '.zip'),
                                   str(new_id) + extension)
                    else:
                        file_df_saved[counter] = {'containerID': new_id, 'filePhase': 0, 'layer': 'base',
                                                  'fileExtension': extension, 'originalName': file.name,
                                                  'originalPath': file.as_posix(),
                                                  'relativePath': (Path(str(
                                                      container_ids[
                                                          file.relative_to(
                                                              extract_dir).parent.as_posix()]) + '.folder') / (
                                                                           str(new_id) + extension)).as_posix(),
                                                  'parentID': container_ids[
                                                      file.relative_to(extract_dir).parent.as_posix()]}
                        copy_files(file, self.base_dir / 'base' / (str(
                            container_ids[file.relative_to(extract_dir).parent.as_posix()]) + '.folder'),
                                   str(new_id) + extension)

                new_id = self.generate_id()
                counter += 1

            del zip_list[0]

        self.database = pd.DataFrame.from_dict(file_df_saved, orient='index')
        print(self.database)

    def set_layer(self, layer_name, layer_description, pipeline_id):

        assert (self.base_dir / layer_name).exists() == False

        make_folder(self.base_dir, layer_name)

        self.layers.append(layer_name)
        temp_layer = Layer(layer_name, layer_description, pipeline_id, self.base_dir / layer_name)
        temp_layer.update_metadata()

    def set_files(self, layer_name):

        ids = {}
        counter = 0
        layer_path = self.base_dir / layer_name
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
            # else:
            # 	try:
            # 		ori_parent = self.database.set_index('containerID')['parentID'].to_dict()[int(file.stem.lstrip('f'))]
            # 	except:
            # 		print('no parent found!!')
            # 		print(file)
            # 	ids[counter] = {'containerID': file.stem.lstrip('f'), 'filePhase': layer_name, \
            # 					'fileExtension': file.suffix, 'originalName': file.name,
            # 					'originalPath': file.as_posix(), \
            # 					'relativePath': file.relative_to(layer_path).as_posix(), \
            # 					'parentID': parent.name}

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
        self.database.to_parquet('database.parquet')

    def read_database(self):
        self.database = pd.read_parquet('database.parquet')

    def update_metadata(self):

        metadata = self.base_dir / 'metadata.json'

        content = {'baseDirectory': self.base_dir.as_posix(), 'description': self.description,
                   'layers': [layer.layer_name for layer in self.layers]}

        with open(metadata.as_posix(), 'w') as f:
            json.dump(content, f)
            f.write('\n')

    def generate_id(self):
        # auto_increment 1

        if len(self.base_files) == 0:
            self.base_files.append(0)
            return 0
        else:
            new_id = sorted(self.base_files)[-1] + 1

            self.base_files.append(new_id)
            return new_id

    def read(self):

        with open(self.base_dir / 'metadata.json', 'r') as f:
            metadata = [json.loads(line) for line in f]

        self.description = metadata[0]['description']

        self.layers = []
        layers = metadata[0]['layers']

        for layer in layers:
            layer_path = self.base_dir / layer

            with open(layer_path / 'metadata.json', 'r') as f:
                layer_data = json.load(f)

            self.layers.append(Layer(layer_data['layer_name'], layer_data['description'], layer_data['pipelineID'],
                                     layer_data['layerDirectory']))

        self.base_files = [int(x) for x in metadata[1].keys()]

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
