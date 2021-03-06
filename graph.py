from datetime import datetime

def calculate_financial_cost(link_data):
	# return (len(link_data['order_count']) + 1) * link_data['cost']
	return link_data['cost']

def calculate_time_cost(link_data, start_date):
	diff = link_data['endDate'] - start_date
	cost = (diff.days * 24) + (diff.seconds/3600)

	return cost

def find_cost(start_date, links, cost_factor):
	link_id = min(
		(link for link, val in links.items() if val['attr_dict']['startDate'] > start_date),
		key=lambda x: links[x]['attr_dict']['startDate']
	)
	link_data = links[link_id]['attr_dict']
	if cost_factor == 'cost':
		cost = calculate_financial_cost(link_data)
	else:
		cost = calculate_time_cost(link_data, start_date)

	return cost, link_id

def find_heuristic_cost():
	return 0

def find_path(g, order_details, cost_factor):
	opened = []
	closed = []

	start_time = order_details['created_on']
	end_node = order_details['destination_zone']
	start_node = order_details['origin_zone']
	current_node = start_node
	opened.append(current_node)
	table = {
		start_node: {
			'cost_to_start': 0,
			'heuristic_dist': 0,
			'f_value': 0,
			'prev_node': None,
			'link_id': None,
			'start_date': start_time,
			'end_date': start_time,
			'label': 'COVERAGEAREA'
		}
	}

	while current_node != end_node:
		# iterate thru adj nodes
		for nbr, links in g[current_node].items():
			# push node to opened list
			if nbr not in opened and nbr not in closed:
				opened.append(nbr)
				cost, link_id = find_cost(start_time, links, cost_factor)
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
					'end_date': links[link_id]['attr_dict']['endDate'],
					'label': g.nodes[nbr]['label']
				}
				# update value if not calculated before or if new f value is lower
				if not table.get(nbr) or (table.get(nbr) and table[nbr]['f_value'] > f):
					table.update({nbr: result})

		# add node to closed list
		opened.remove(current_node)
		if current_node not in closed:
			closed.append(current_node)
		# set next opened node to current node
		current_node = max((key for key in table.keys() if key in opened), key=lambda x: table[x]['f_value'])
		start_time = table[current_node]['end_date']

	current_node = end_node
	links = []

	while current_node != start_node:
		prev_node = table[current_node]['prev_node']
		link_id = table[current_node]['link_id']
		link_data = g[prev_node][current_node][link_id]['attr_dict']
		links.append(((prev_node, table[prev_node]['label']), (current_node, table[current_node]['label']), link_data))
		current_node = prev_node

	return links, table[end_node]['cost_to_start']

