from datetime import datetime

import networkx as nx

def calculate_financial_cost(link_data):
	pass

def calculate_time_cost(link_data):
	pass

def find_cost(start_date, links, cost_factor):
	for link_id,link in links.items():
		print(start_date < link['attr_dict']['startDate'])

	return 12, 0

def find_heuristic_cost():
	return 0

def find_path(g, order_details):
	open = []
	closed = []

	created_on = order_details['created_on'][:-3] + order_details['created_on'][-2:]
	start_time = datetime.strptime(created_on, '%Y-%m-%dT%H:%M:%S.%f%z')
	end_node = order_details['destination_zone']
	start_node = order_details['origin_zone']
	current_node = start_node
	open.append(current_node)
	table = {
		start_node: {
			'cost_to_start': 0,
			'heuristic_dist': 0,
			'f_value': 0,
			'prev_node': None,
			'link_id': None,
			'start_date': None,
			'end_date': start_time
		}
	}

	while current_node != end_node:
		print(f'\ncurrent: {current_node}\nclosed:{closed}\nopen: {open}')
		# iterate thru adj nodes
		for nbr, links in g[current_node].items():
			# push node to open list
			print(f'nbr: {nbr}')
			if nbr not in open and nbr not in closed:
				open.append(nbr)
				cost,link_id = find_cost(start_time, links, 'duration')
				h = find_heuristic_cost()
				if current_node != start_node:
					cost_to_start = table[current_node]['cost_to_start'] + cost
				else:
					cost_to_start = cost
				f = cost_to_start + h
				result = {
					'cost_to_start': cost_to_start,
					'heuristic_dist': h,
					'f_value': f,
					'prev_node': current_node,
					'link_id': link_id,
					'start_date': links[link_id]['attr_dict']['startDate'],
					'end_date': links[link_id]['attr_dict']['endDate']
				}
				# update value if not calculated before or if new f value is lower
				if not table.get(nbr) or (table.get(nbr) and table[nbr]['f_value'] > f):
					table.update({nbr: result})

		# add node to closed list
		open.remove(current_node)
		if current_node not in closed:
			closed.append(current_node)
		# set next open node to current node
		current_node = max(table, key=lambda x:table[x]['f_value'])
		prev_node = table[current_node]['prev_node']
		start_time = table[prev_node]['end_date']


