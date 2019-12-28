import re

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
		self._driver = GraphDatabase.driver(self.uri, auth=self.credentials)

	def _finish(self):
		self._driver.close()

	def clear_graph(self, tx):
		print('clearing graph...')
		tx.run('MATCH (n) DETACH DELETE n;')
		print('graph cleared!')

	def _update_state(self, session):
		pass

	@staticmethod
	def create_graph(tx, nodes, links):
		print('creating graph...')
		query = ''
		for node in nodes:
			label = node.pop('label')
			node_name = re.sub("[^a-zA-Z]+", "", node['name'])
			node_query = f'merge ({node_name}:{label}{{name:{node["name"]}{node.get("attr","")}}})\n'
			query += node_query.replace('”', '"').replace("’", "'")
		for link in links:
			node1_name = re.sub("[^a-zA-Z]+", "", link['node1'])
			node2_name = re.sub("[^a-zA-Z]+", "", link['node2'])
			new_link = f'merge ({node1_name})-[:{link["link"]}{{{link["attr"]}}}]->({node2_name})\n'
			query += new_link
		final_query = query.replace('”', '"').replace("’", "'")[:-1]
		res = tx.run(final_query)
		print('graph created!')
		return res

	def build_graph(self, nodes, links, clear_graph):
		with self._driver.session() as session:
			if clear_graph:
				session.write_transaction(self.clear_graph)

			session.write_transaction(self.create_graph, nodes, links)

	def run_algo(self):
		pass