import re
import time

from neo4j import GraphDatabase

def time_and_rollback(func):

	def wrapper_func(*args, **kwargs):
		start = time.time()
		res = func(*args, **kwargs)
		end = time.time()
		res['time'] = end - start
		return res

	return wrapper_func

class DBHandler:
	"""
	class to handle all neo4j queries
	driver will be instantiated and closed here
	all queries will exist here
	"""

	uri = "bolt://localhost:7687"
	credentials = ("neo4j", "router123")
	order_fields = ['consignee_city', 'pickup_city']
	query_field_mapping = {
		'consignee_city': 'to_name',
		'pickup_city': 'from_name'
	}

	algo = \
"""MATCH (from:from_label {name:$from_name}), (to:CITY {name:$to_name}) ,
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
			query += node_query.replace('”', '"').replace("’", "'").replace('‘', "'")
		for link in links:
			node1_name = re.sub("[^a-zA-Z]+", "", link['node1'])
			node2_name = re.sub("[^a-zA-Z]+", "", link['node2'])
			new_link = f'merge ({node1_name})-[:{link["link"]}{{{link["attr"]}}}]->({node2_name})\n'
			query += new_link
		final_query = query.replace('”', '"').replace("’", "'").replace('‘', "'")[:-1]
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

	def _pre_find_path(self):
		pass

	def _post_find_path(self):
		pass

	@time_and_rollback
	def _find_path(self, tx, **kwargs):
		path_result = {'path': None}
		st = self.algo.replace('from_label', kwargs.pop('from_label'))
		transaction = tx.run(st, **kwargs)
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
		return path

	def build_graph(self, nodes, links, clear_graph):
		with self._driver.session() as session:
			if clear_graph:
				session.write_transaction(self._clear_graph)

			session.write_transaction(self._create_graph, nodes, links)

	def run_algo(self, order_data, static):
		order_kwargs = {'from_label': 'CITY'}
		total_result = []
		for key in self.order_fields:
			query_key = self.query_field_mapping.get(key, key)
			order_kwargs[query_key] = order_data[key]
		session = self._driver.session()
		tx = session.begin_transaction()
		result = {
			'tracking_no': order_data.get('tracking_no'),
			'price_factor': 0,
			'time_factor': 0,
			'conditions': None
		}
		path_result = self._find_path(tx, **order_kwargs)
		path = path_result.pop('path')
		if path:
			result['path'] = self._format_path(path)
			result.update(path_result)
		else:
			result['path'] = 'No path found'
			return [result]

		if static:
			tx.rollback()
			return [result]
		else:
			total_result.append(result)
			current_node = path.nodes[1]
			end_node = path.end_node

			while current_node['name'] != end_node['name']:
				order_kwargs['from_label'] = list(current_node.labels)[0]
				order_kwargs['from_name'] = current_node['name']

				result = {
					'tracking_no': order_data.get('tracking_no'),
					'price_factor': 0,
					'time_factor': 0,
					'conditions': None
				}
				path_result = self._find_path(tx, **order_kwargs)
				path = path_result.pop('path')
				if path:
					result['path'] = self._format_path(path)
					result.update(path_result)
					total_result.append(result)
					current_node = path.nodes[1]
				else:
					result['path'] = 'No path found'
					total_result.append(result)
					tx.rollback()
					return total_result

			tx.rollback()
			return total_result
