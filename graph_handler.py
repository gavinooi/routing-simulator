from neo4j import GraphDatabase

class GraphHandler:
	"""
	class to handle all neo4j queries
	driver will be instantiated and closed here
	all queries will exist here
	"""

	uri = "bolt://localhost:7687"
	credentials = ("neo4j", "router123")
	algo = """
	
	"""

	def __init__(self):
		self.driver = GraphDatabase.driver(self.uri, auth=self.credentials)

	def finish(self):
		self.driver.close()

	def build_graph(self, nodes, links, clear_graph):
		if clear_graph:
			self._clear_graph()



	def _clear_graph(self, session):
		print('graph cleared')

	def _update_state(self, session):
		pass

	def run_algo(self):
		pass