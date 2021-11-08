from pathlib import Path
import json
from glob import glob
from shutil import copy, rmtree
from layer import Layer

class Base:
	def __init__(self, base_dir, description = None):
		self.base_dir = Path(base_dir)
		self.description = description
		self.base_files = []
		self.filephases ={}
		self.layers = []


	def prepare_base(self, file_dir):

		files = get_all_files(file_dir)

		for file in files:

			extension = file.split('.')[-1]
			new_id = self.generate_id()

			self.base_files.append(new_id + '.' + extension)

			copy_files(file, self.base_dir / 'base', new_id + '.' + extension )

			self.filephases[new_id] = {'filephases': [], 'layers': []}


		self.update_metadata()

	def update_metadata(self):

		metadata = self.base_dir / 'metadata.json'

		content = {'baseDirectory': self.base_dir, 'description': self.description, 'layers' : [(layer.name, layer.description) for layer in self.layers]}

		with open(metadata.text, 'w') as f:
			json.dump(content, f)

			json.dump(self.filephases)



	def set_files(self, layer_name):
		layer_dir = self.base_dir / layer_name

		# filenames are fileID - filephase

		files = get_all_files(layer_dir.text)

		fileIDs = dict([(file.split('-')[0], file.split('-')[-1].split('.')[0]) for file in files])

		for base_file in self.base_files:

			if base_file in fileIDs:

				self.filephases[base_file]['filephases'].append(fileIDs[base_file])
				self.filephases[base_file]['layers'].append(layer_name)
			else:
				self.filephases[base_file]['filephases'].append(None)
				self.filephases[base_file]['layers'].append(None)


	def eliminate_layers(self, layer_name):

		index = [layer.name for layer in self.layers].index(layer_name)

		to_delete = self.layers[index + 1:]
		self.layers = self.layers[:index + 1]

		for file in self.base_files:
			self.base_files[file]['filephases'] = self.base_files[file]['filephases'][:index + 1]
			self.base_files[file]['layers'] = self.base_files[file]['layers'][:index + 1]

		for folder in to_delete:
			remove_folder(folder.directory)

	def set_layer(self, layer_description, pipeline_id, directory):
		self.layers.append(Layer(layer_name, layer_description, pipeline_id, directory))

		self.layers[-1].update_metadata()


	def return_current_layers(self):
		return dict([(key, lastlayer['layers'][-1]) for key, lastlayer in filephases.items()])

	def return_current_phases(self):
		return dict([(key, lastlayer['filephases'][-1]) for key, lastlayer in filephases.items()])


	def return_build(self):
		return  dict([(key, lastlayer['filephases']) for key, lastlayer in filephases.items()])

	def generate_id(self):

		new_id = sorted(self.base_files)[-1] + 1

		self.base_files.append(new_id)
		return 'f' + str(new_id)


def get_all_files(directory):

	return glob(Path(directory).text + '/*/**')

def copy_files(file_dir, to_dir, filename):

	copy(Path(file_dir), Path(to_dir) / filename)

def remove_folder(directory):
	rmtree(directory)



