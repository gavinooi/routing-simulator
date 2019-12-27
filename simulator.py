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
			attr = {att.split(':')[0]: att.split(':')[1] for att in row[2].replace(' ', '').split(',')} if row[2] else {}
			attr.update({'name':f'"{row[0]}"', 'label':row[1]})
			nodes.append(attr)

		for row in link_sheet.iter_rows(min_row=2, values_only=True):
			attr = {att.split(':')[0]: att.split(':')[1] for att in row[3].replace(' ', '').split(',')} if row[3] else {}
			attr.update(
				{'node1':row[0], 'node1_label':row[1], 'link':row[2], 'node2':row[4], 'node2_label':row[5]}
			)
			links.append(attr)

		self.gh.build_graph(nodes, links, clear_graph)


	def _update_graph(self):
		pass

	def _output_result(self, output_name=None):
		pass

	def run_simulation(self):
		print('simulation started')
		print('simulation finished')
