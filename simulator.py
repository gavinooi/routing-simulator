import csv

from openpyxl import load_workbook

from graph_handler import GraphHandler

class Simulator:

	results = []

	def __init__(self, graph_file, orders_file, algo='STATIC',output_file='result', clear_graph=True):
		# clear the previous graph
		# build graph
		# take in the algo
		self.gh = GraphHandler()
		self.orders = self._load_orders(orders_file)
		self._build_graph(graph_file, clear_graph)

	def _load_orders(self, orders_file):
		with open(f'{orders_file}.csv') as orders:
			reader = csv.DictReader(orders)
			return [dict(row) for row in reader]


	def _finish(self):
		self.gh.finish()

	def _build_graph(self, graph_file, clear_graph):
		workbook = load_workbook(f'{graph_file}.xlsx')
		node_sheet = workbook['nodes']
		nodes = []
		link_sheet = workbook['links']
		links = []

		for row in node_sheet.iter_rows(min_row=2, values_only=True):
			attr = {'name': f'"{row[0]}"', 'label': row[1]}
			if row[2]:
				attr['attr'] = f', {row[2]}'
			nodes.append(attr)

		for row in link_sheet.iter_rows(min_row=2, values_only=True):
			attr= {
				'node1':f'"{row[0]}"',
				'node1_label':row[1],
				'link':row[2],
				'node2':f'"{row[4]}"',
				'node2_label':row[5],
				'attr': row[3]
			}
			links.append(attr)

		self.gh.build_graph(nodes, links, clear_graph)


	def _update_graph(self):
		pass

	def _output_result(self, output_name=None):
		pass

	def run_simulation(self):
		print('simulation started')
		print('simulation finished')
