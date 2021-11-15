from pathlib import Path
import json
from shutil import copy, rmtree
from layer import Layer
class Base:
	def __init__(self, base_dir, description = None):
		self.base_dir = Path(base_dir)
		self.description = description
		self.base_files = []
		self.filephases ={}
		self.layers = []
		self.database = None


	def prepare_base(self, file_dir):

		files = get_all_files(file_dir)
		try:
			make_folder(self.base_dir, 'base')
		except FileExistsError:
			pass 


		to_be_saved = {}
		counter = 0
		for file in files:

			for folder in file.relative_to(file_dir).

			to_be_saved[counter] = {'fileID': str(new_id), 'containerID': 'root', 'filePhase': 'base', \
			'fileExtension': extension, 'originalName': file.name, 'originalFilePath': file.as_posix()}

			extension = file.suffix
			new_id = self.generate_id()


			copy_files(file, self.base_dir / 'base', 'f' + str(new_id) +  extension, relative_to = file_dir)

			# self.filephases[str(new_id)] = {'filephases': [], 'layers': []}

		self.update_metadata()

		self.database = pd.DataFrame(to_be_saved)


	def update_metadata(self):

		metadata = self.base_dir / 'metadata.json'

		content = {'baseDirectory': self.base_dir.as_posix(), 'description': self.description, 'layers' : [layer.layer_name for layer in self.layers]}

		with open(metadata.as_posix(), 'w') as f:
			json.dump(content, f)
			f.write('\n')
			json.dump(self.filephases, f)


	def set_files(self, layer_name):

		if layer_name == 'base':

			for base_file in self.base_files:

				self.filephases[str(base_file)]['filephases'].append(0)
				self.filephases[str(base_file)]['layers'].append('base')

		else:
			layer_dir = self.base_dir / layer_name

			# filenames are fileID - filephase

			files = get_all_files(layer_dir.as_posix())


			fileIDs = dict([(Path(file).name.split('-')[0].split('f')[-1], int(Path(file).name.split('-')[-1].split('.')[0].split('p')[-1])) for file in files])

			for base_file in self.base_files:

				if str(base_file) in fileIDs:

					self.filephases[str(base_file)]['filephases'].append(fileIDs[str(base_file)])
					self.filephases[str(base_file)]['layers'].append(layer_name)
				else:
					self.filephases[str(base_file)]['filephases'].append(None)
					self.filephases[str(base_file)]['layers'].append(None)


		self.update_metadata()
	def eliminate_layers(self, layer_name):

		index = [layer.name for layer in self.layers].index(layer_name)

		to_delete = self.layers[index + 1:]
		self.layers = self.layers[:index + 1]

		for file in self.base_files:
			self.base_files[file]['filephases'] = self.base_files[file]['filephases'][:index + 1]
			self.base_files[file]['layers'] = self.base_files[file]['layers'][:index + 1]

		for folder in to_delete:
			remove_folder(folder.directory)

	def set_layer(self, layer_description, pipeline_id, layer_name):

		make_folder(self.base_dir, layer_name)

		self.layers.append(Layer(layer_name, layer_description, pipeline_id, self.base_dir / layer_name))

		self.layers[-1].update_metadata()

		self.update_metadata()


	def return_current_layers(self):
		return dict([(key, lastlayer['layers'][-1]) for key, lastlayer in self.filephases.items()])

	def return_current_phases(self):
		return dict([(key, lastlayer['filephases'][-1]) for key, lastlayer in self.filephases.items()])


	def return_build(self):
		return  dict([(key, lastlayer['filephases']) for key, lastlayer in self.filephases.items()])

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

			self.layers.append(Layer(layer_data['layer_name'], layer_data['description'], layer_data['pipelineID'], layer_data['layerDirectory']))

		self.base_files = [int(x) for x in metadata[1].keys()]

		self.filephases = metadata[1]


def get_all_files(directory):

	p = Path(directory)
	filtered = [x.relative_to(directory) for x in p.glob("**/*") if (not x.name.startswith('.'))]

	return filtered

def copy_files(file_dir, to_dir, filename, relative_to = None):


	new_path = to_dir / file_dir
	new_path.parent.mkdir(parents = True, exist_ok = True)
	if relative_to:
		file_dir = relative_to / file_dir
	copy(file_dir, new_path.parent / filename)

def make_folder(file_dir, name):
	new_folder = file_dir / name

	new_folder.mkdir(parents = True)

def remove_folder(directory):
	rmtree(directory)


