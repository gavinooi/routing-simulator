import argparse

from simulator import Simulator

parser = argparse.ArgumentParser(description='CLI for Network-Partner Routing Simulator')

parser.add_argument('graph', metavar='graph_file', type=str, help='File name of graph excel file')
parser.add_argument('order', metavar='order_file', type=str, help='File name of order data csv file')
parser.add_argument('-output', metavar='output_file', type=str, help='File name of output excel file')
parser.add_argument('-config', metavar='config', type=str, help='Optional configurations')

args = parser.parse_args()

graph_file = args.graph
order_file = args.order
optional_args = {'output_file': 'output.xlsx', 'cost_factor': 'time'}
config_mapping = {
	'd': ('algo', 'DYNAMIC'),
	'k': ('clear_graph', False),
	'p': ('cost_factor', 'cost')
}
if args.output:
	optional_args['output_file'] = f'{args.output}.xlsx'
configs = args.config if args.config else ''
for k in config_mapping.keys():
	if k in configs:
		config = config_mapping[k]
		optional_args[config[0]] = config[1]

sim = Simulator(graph_file, order_file, **optional_args)
sim.run_simulation()