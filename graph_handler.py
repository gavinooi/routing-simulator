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
	order_fields = []
	algo = """
MATCH (from: CITY {name: 'Hougang'}), (to: CITY {name: 'Manila'}) ,
path = (from)-[:CONNECTED_TO*]->(to)
WITH REDUCE(dist = 0, rel in rels(path) | dist + rel.cost) AS cost, path
RETURN path, cost
ORDER BY cost
LIMIT 1
"""

	def __init__(self):
		self._driver = GraphDatabase.driver(self.uri, auth=self.credentials)

	def finish(self):
		self._driver.close()

	@staticmethod
	def _clear_graph(tx):
		print('clearing graph...')
		tx.run('MATCH (n) DETACH DELETE n;')
		print('graph cleared!')

	def _update_state(self, session):
		pass

	@staticmethod
	def _create_graph(tx, nodes, links):
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

	@staticmethod
	def _update_count(tx, links):
		match_query = 'match '
		set_query = '\nset '
		for link in links:
			start_node = link.nodes[0]
			start_label = ':'.join(start_node.labels)
			start_name = start_node['name']
			end_node = link.nodes[1]
			end_label = ':'.join(end_node.labels)
			end_name = end_node['name']
			link_label = re.sub("[^a-zA-Z]+", "", (start_name+end_name).strip())
			match_query += f'\n(:{start_label}{{name:"{start_name}"}})-[{link_label}:{link.type}]->(:{end_label}{{name:"{end_name}"}}),'
			set_query += f'{link_label}.order_count = {link_label}.order_count + 1, '
		final_query = match_query[:-1] + set_query[:-2]
		tx.run(final_query)

	def _find_path(self, tx):
		path_result = {'path': None}
		transaction = tx.run(self.algo)
		path_result['query'] = transaction.summary().statement
		result = transaction.single()
		if not result:
			return path_result
		else:
			result = result.data()
			path_result['path'] = result['path']
			path_result['cost'] = result['cost']
			return path_result

	@staticmethod
	def _format_path(path_obj):
		links = path_obj.relationships
		path = f'({path_obj.start_node["name"]})'
		for link in links:
			path += f'- {link.type} -> ({link.nodes[1]["name"]})'
		print(path)
		return path

	def build_graph(self, nodes, links, clear_graph):
		with self._driver.session() as session:
			if clear_graph:
				session.write_transaction(self._clear_graph)

			session.write_transaction(self._create_graph, nodes, links)

	def run_algo(self, order_data, static):
		with self._driver.session() as session:
			if static:
				result = {
					'tracking_no': order_data.get('tracking_no'),
					'price_factor': 0.7,
					'time_factor': 0.3,
					'conditions': None
				}
				path_result = session.write_transaction(self._find_path)
				if path_result['path']:
					result['path'] = self._format_path(path_result['path'])
					session.write_transaction(self._update_count, path_result.pop('path').relationships)
					result.update(path_result)
				return [result]

			else:
				'''
				while current node != endnode
					find the path
					update the state of the first node, update the new start point with the other conditions
					format the path data and add it to list of results
				return array of results'''
