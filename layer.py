from pathlib import Path


class Layer:
	def __init__(self, layer_description, pipeline_id, directory):
		self.pipeline_id = pipeline_id
		self.layer_description = layer_description
		self.directory = Path(directory)


	def update_metadata(self):
		metadata = self.directory / 'metadata.json'

    	content = {'layerDirectory': self.directory, 'description': self.layer_description, 'pipelineID': self.pipeline_id}

    	with open(metadata.text, 'w') as f:
    		json.dump(content, f)





