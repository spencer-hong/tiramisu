from pathlib import Path
# from base import make_folder
import json
class Layer:
	def __init__(self, layerName, layerDescription, pipelineID, directory):
		self.pipelineID = pipelineID
		self.layerDescription = layerDescription
		self.directory = Path(directory)
		self.layerName = layerName

	def update_metadata(self):
		metadata = self.directory / 'metadata.json'

		content = {'layerName': self.layerName, 'layerDirectory': self.directory.as_posix(), 'description': self.layerDescription, 'pipelineID': self.pipelineID}

		with open(metadata.as_posix(), 'w') as f:
			json.dump(content, f)

			







