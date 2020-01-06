import csv
from datetime import datetime

from openpyxl import load_workbook, Workbook

from graph_handler import GraphHandler

class Simulator:

	results = []
	output_file = 'output.xlsx'

	def __init__(self, graph_file, orders_file, algo='STATIC', clear_graph=True):
		self.static = algo == 'STATIC'
		print(
			'###############################\n'
			'### JANIO ROUTING SIMULATOR ###\n'
			'###############################\n'
			f'\nCONFIGURATIONS:\nGraph file:  {graph_file}\nOrder file:  {orders_file}.csv\nOutput file: {self.output_file}.xlsx\n'
			f'\nADDITIONAL CONFIGURATIONS\nAlgo: {algo}\nClear graph: {clear_graph}'
		)
		self.sheet_name = datetime.now().strftime("%d-%m T%H-%M-%S")
		self.gh = GraphHandler()
		self.orders = self._load_orders(orders_file)
		self._build_graph(graph_file, clear_graph)

	def _load_orders(self, orders_file):
		orders_list = []
		with open(f'{orders_file}.csv') as orders:
			reader = csv.DictReader(orders)
			for row in reader:
				order_data = {}
				for key,val in row.items():
					order_data[key.lower().replace(' ', '_')] = val
				orders_list.append(order_data)
		print(f'ORDERS LOADED: {len(orders_list)}\n')
		return orders_list

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
				'attr': row[3] + ', order_count:0'
			}
			links.append(attr)

		self.gh.build_graph(nodes, links, clear_graph)

	def _output_result(self):
		try:
			workbook = load_workbook(self.output_file)
			sheet = workbook.create_sheet(self.sheet_name)
		except:
			workbook = Workbook()
			sheet = workbook.active
			sheet.title = self.sheet_name
		finally:
			sheet.append(('order', 'start', 'end', 'cost_factor'))
			for result in self.results:
				sheet.append(list(result.values()))

			orders = {}
			for row in sheet.iter_rows(min_row=1,max_col=1):
				cell = row[0]
				if orders.get(cell.value):
					orders[cell.value].append(cell.coordinate)
				else:
					orders[cell.value] = [cell.coordinate]

			for k, v in orders.items():
				if len(v) > 1:
					sheet.merge_cells(f'{v[0]}:{v[-1]}')
			workbook.save(self.output_file)

	def run_simulation(self):
		print('SIMULATION STARTED')
		for count, order in enumerate(self.orders):
			print(f'RUNNING ORDER {count+1}/{len(self.orders)}', end='\r')
			result = self.gh.run_algo(order, self.static)
			self.results.extend(result)

		self._output_result()
		self._finish()
		print('\nSIMULATION FINISHED')
