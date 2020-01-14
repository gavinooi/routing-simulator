import csv
from datetime import datetime
import bisect

import networkx as nx

from openpyxl import load_workbook, Workbook

from graph import find_path
from db_handler import DBHandler

class Simulator:

	results = []
	timeline = []

	def __init__(self, graph_file, orders_file, output_file, algo='STATIC', clear_graph=True):
		self.static = algo == 'STATIC'
		self.output_file = output_file
		print(
			'###############################\n'
			'### JANIO ROUTING SIMULATOR ###\n'
			'###############################\n'
			f'\nCONFIGURATIONS:\nGraph File:  {graph_file}\nOrder File:  {orders_file}.csv\nOutput File: {self.output_file}\n'
			f'\nADDITIONAL CONFIGURATIONS\nAlgo: {algo}\nClear graph: {clear_graph}'
		)
		self.sheet_name = datetime.now().strftime("%d-%m T%H-%M-%S")
		self.handler = DBHandler()
		self.orders = self._load_orders(orders_file)
		self._build_graph(graph_file, clear_graph)

	### SETUP ###

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
		self.handler.finish()

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

		self.handler.build_graph(nodes, links, clear_graph)

	def _output_result(self):
		try:
			workbook = load_workbook(self.output_file)
			sheet = workbook.create_sheet(self.sheet_name)
		except:
			workbook = Workbook()
			sheet = workbook.active
			sheet.title = self.sheet_name
		finally:
			sheet.append(list(self.results[0].keys()))
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

	### TIMELINE ###

	def add_event(self, event):
		'''
		adds an event into the timeline, in a sorted order
		event = {
			datetime: time the event occurs
			type: [create, arrive, leave, expire]
			desc: description of the event (with some order specific data like tracking no)
			kwargs: dictionary of keyword args to be passed into the pre stage
		}
		:return:
		'''
		event_actions = {
			'create': {
				'pre': None,
				'during': self.create_order,
				'post': self.increment_order_count
			},
			'arrive': {
				'pre': self.insert_noise,
				'during': self.reach_node,
				'post': self.increment_order_count
			},
			'leave': {
				'pre': None,
				'during': self.decrement_order_count,
				'post': None
			},
			'expire': {
				'pre': None,
				'during': self.expire_link,
				'post': None
			},
		}
		self.timeline.append(event)

	def add_create_order_event(self, order):
		# add the create order event into the timeline
		tracking_no = order['tracking_no']
		created_on = order['created_on']
		origin = order['origin_zone']
		destination = order['destination_zone']
		kwargs = {
			'tracking_no': tracking_no,
			'created_on': created_on,
			'agent_app': order['agent_application_name'],
			'payment_type': order['payment_type'],
			'origin_zone': origin,
			'destination_zone': destination
		}
		event = {
			'datetime': created_on,
			'event_type': 'create',
			'desc': f'Create order: {tracking_no}, {origin} --> {destination}',
			'actions': [self.create_order, self.increment_order_count],
			'kwargs': kwargs
		}

		self.add_event(event)

	def consume_event(self, event):
		'''
		runs the pre, during & post stage methods, logs event to the timeline
		:return:
		'''
		kwargs = event['kwargs']
		for action in event['actions']:
			kwargs = action(**kwargs)

	def run_timeline(self):
		print(len(self.timeline))
		while len(self.timeline) != 0:
			self.consume_event(self.timeline.pop(0))

	### ACTIONS ###

	def insert_noise(self):
		pass

	def expire_link(self):
		pass

	def create_order(self, **kwargs):
		# load the sub graph
		sub_graph = self.handler.filter_graph(kwargs)
		g = nx.MultiDiGraph()
		for r in sub_graph.relationships:
			from_node = r.nodes[0]
			g.add_node(from_node['name'], **from_node._properties)
			to_node = r.nodes[1]
			g.add_node(to_node['name'], **to_node._properties)
			g.add_edge(from_node['name'], to_node['name'], attr_dict=r._properties)

		# find the path
		path,cost, links = find_path(g, kwargs)
		print(links)
		self.handler.increment_links(kwargs['tracking_no'], links)
		result = {
			'tracking_no': kwargs.get('tracking_no'),
			'price_factor': 0,
			'time_factor': 0,
			'conditions': None,
			'path': path,
			'cost': cost
		}


		# add the arrive & leave events to the timeline
		return {'some shit': 'this is the result'}

	def increment_order_count(self, **kwargs):
		# add order to a timed link
		# happens when a new path is generated (order creation, path continuation & updating order count)
		print(f'increment order\n{kwargs}')

	def decrement_order_count(self, **kwargs):
		# remove order from a timed link
		# happens when an order is picked up or when updating order counts
		pass

	def update_order_count(self, **kwargs):
		# occurs when the order is first created, when it arrives at
		pass

	def reach_node(self, **kwargs):
		# find the next n+2 path if it is janio or not
		# reroute if thr is a cancellation api
		pass

	def run_simulation(self):
		print('\nBUILDING TIMELINE')
		for count, order in enumerate(self.orders):
			print(f'RUNNING ORDER {count+1}/{len(self.orders)}', end='\r')
			self.add_create_order_event(order)

		self.run_timeline()

		# self._output_result()
		self._finish()
		print('\nSIMULATION FINISHED')
