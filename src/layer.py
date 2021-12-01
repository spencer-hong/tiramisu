from pathlib import Path
# from base import make_folder
import json
class Layer:
	def __init__(self, layer_name, layer_description, pipeline_id, directory):
		self.pipeline_id = pipeline_id
		self.layer_description = layer_description
		self.directory = Path(directory)
		self.layer_name = layer_name

	def update_metadata(self):
		metadata = self.directory / 'metadata.json'

		content = {'layerName': self.layer_name, 'layerDirectory': self.directory.as_posix(), 'description': self.layer_description, 'pipelineID': self.pipeline_id}

		with open(metadata.as_posix(), 'w') as f:
			json.dump(content, f)


	# def prepare_layer(self, list_of_files):

	# 	for file in list_of_file:
			







