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

	def _clear_graph(self, tx):
		print('clearing graph...')
		tx.run('MATCH (n) DETACH DELETE n;')
		print('graph cleared!')

	def _update_state(self, session):
		pass

	@staticmethod
	def create_nodes_query(tx, nodes):
		print('creating nodes...')
		query = 'create '
		for node in nodes:
			label = node.pop('label')
			new_node = f'(:{label}{{'
			for key,val in node.items():
				new_node += f'{key}:{val},'
			new_node = new_node[:-1] + '}),\n'
			query += new_node
		res = tx.run(query.replace('”', '"')[:-2]).value()
		print('nodes created!')
		return res

	@staticmethod
	def create_links_query(tx, links):
		query = 'create '
		for node in links:
			pass
		return tx.run(query).value()

	def build_graph(self, nodes, links, clear_graph):
		with self._driver.session() as session:
			if clear_graph:
				session.write_transaction(self._clear_graph)

			result = session.write_transaction(self.create_nodes_query, nodes)
			# link_query = self.create_links_query(links)

	def run_algo(self):
		pass