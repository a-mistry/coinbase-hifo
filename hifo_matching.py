import requests
import datetime
import time
import base64
import hmac
import hashlib
import json
import csv
import sys

products = ['BTC-USD','ETH-USD','LTC-USD','BCH-USD']
api_key = 'YOUR_API_KEY_HERE'
secret_key = 'YOUR_SECRET_HERE'
passphrase = 'YOUR_PASSPHRASE_HERE'
EPSILON = 0.00000005

def get_auth_headers(timestamp, message, api_key, secret_key, passphrase):
	message = message.encode('ascii')
	hmac_key = base64.b64decode(secret_key)
	signature = hmac.new(hmac_key, message, hashlib.sha256)
	signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')
	return {
		'Content-Type': 'application/json',
		'CB-ACCESS-KEY': api_key,
		'CB-ACCESS-SIGN': signature_b64,
		'CB-ACCESS-TIMESTAMP': timestamp,
		'CB-ACCESS-PASSPHRASE': passphrase,	
	}

def save_reports():
	global api_key
	global secret_key
	global passphrase
	global products

	for product_id in products:
		timestamp = str(time.time())
		method = 'POST'
		path_url = '/reports'
		body = json.dumps({
			'type': 'fills',
			'start_date': '2018-01-01T00:00:00.000Z',
			'end_date': '2019-01-01T00:00:00.000Z',
			'product_id': product_id,
			'format': 'csv'
		})
		message = ''.join([timestamp, method, path_url, (body or '')])
		headers = get_auth_headers(timestamp, message, api_key, secret_key, passphrase)
		url = 'https://api.pro.coinbase.com' + path_url
		accounts_json = requests.post(url, headers=headers, data=body).json()
		print(str(accounts_json))
		# returns something like {'type': 'fills', 'id': '4744cd87-ebf4-4222-a058-ef054c1432a2', 'status': 'pending'}

		status_json = {'status': 'creating'}
		while status_json['status'] != 'ready':
			timestamp = str(time.time())
			method = 'GET'
			path_url = '/reports/' + accounts_json['id']
			body = None
			message = ''.join([timestamp, method, path_url, (body or '')])
			headers = get_auth_headers(timestamp, message, api_key, secret_key, passphrase)
			url = 'https://api.pro.coinbase.com' + path_url
			status_json = requests.get(url, headers=headers).json()
			print(str(status_json))
			time.sleep(2)

		data = requests.get(status_json['file_url'], stream=True)
		with open('fills-'+product_id+'.csv', 'wb') as fd:
			for chunk in data.iter_content(chunk_size=128):
				fd.write(chunk)

def calc_hifo():
	global EPSILON
	global products

	matches = []
	for product_id in products:
		print('Calculating accounting on product ' + product_id)
		with open('fills-'+product_id+'.csv', 'r') as input_file:
			csv_reader = csv.DictReader(input_file)

			purchases = []
			count = 0
			for row in csv_reader:
				count += 1
				size = float(row['size'])
				price = float(row['price'])
				trade_id = row['trade id']
				date = row['created at']
				if row['side'] == 'BUY':
					record = [size,price,trade_id,date]
					inserted = False
					for i in range(0,len(purchases)):
						if price > purchases[i][1]:
							purchases.insert(i, record)
							inserted = True
							break
					if not inserted:
						purchases.append(record)
				else:
					while size>EPSILON:
						purchase = purchases[0]
						if size<purchase[0]+EPSILON:
							purchase[0] -= size
							match_size = size
							size = 0
						else:
							size -= purchase[0]
							match_size = purchase[0]
							purchase[0] = 0
						if purchase[0]<EPSILON:
							purchases.pop(0)
						match = {
							'product': product_id,
							'purchase_date': purchase[3],
							'purchase_trade_id': purchase[2],
							'purchase_price': purchase[1],
							'quantity': match_size,
							'sale_date': date,
							'sale_trade_id': trade_id,
							'sale_price': price,
							'cost_basis': purchase[1]*match_size,
							'proceeds': price*match_size,
							'gain': (price-purchase[1])*match_size
						}
						matches.append(match)

			if len(purchases)>0:
				print('Not all purchases had a matching sale')
				print(str(purchases))
				sys.exit(1)

			print('Read ' + str(count) + ' rows from fills-' + product_id + '.csv')

	with open('crypto-matches.csv', 'w', newline='') as output_file:
		output_cols = ['product','purchase_date','purchase_trade_id','purchase_price','quantity','sale_date','sale_trade_id','sale_price','cost_basis','proceeds','gain']
		csv_writer = csv.DictWriter(output_file, fieldnames=output_cols)
		csv_writer.writeheader()
		for match in matches:
			csv_writer.writerow(match)
	print('Wrote crypto-matches.csv')

def main():
	save_reports()
	calc_hifo()

if __name__ == '__main__':
	main()