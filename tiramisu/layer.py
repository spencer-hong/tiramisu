from pathlib import Path
import pandas as pd
# from base import make_folder
import json

# create a database that is layer specific
class Layer:
	def __init__(self, layerName, layerDescription, pipelineID, directory):
		self.pipelineID = pipelineID
		self.layerDescription = layerDescription
		self.directory = Path(directory)
		self.layerName = layerName
		self.database = None


	def update_metadata(self):
		metadata = self.directory / 'metadata.json'

		content = {'layerName': self.layerName, 'layerDirectory': self.directory.as_posix(), 'description': self.layerDescription, 'pipelineID': self.pipelineID}

		with open(metadata.as_posix(), 'w') as f:
			json.dump(content, f)

	#df must be have a containerID field
	def create_layer_database(self, df):
		assert 'containerID' in df.columns.to_list()

		self.database = df
		self.dump_database()

	def dump_database(self):

		self.database.to_parquet( self.directory / 'database.parquet')

	def read_database(self):
		self.database = pd.read_parquet(self.directory/'database.parquet')

		return self.database
