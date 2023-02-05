import json
import requests
import threading
import websocket
import time

currency_url = 'https://api.poloniex.com/markets'
answer = requests.get(currency_url)
lock = threading.Lock()
currencies = answer.json()

list_currencies = list()
for currency in currencies:
    if currency['state'] == 'NORMAL':
        list_currencies.append(currency['symbol'])


def get_unix_time():
    return round(time.time() * 1000)


def sub_trades(ws):
    ws.send(json.dumps({
        "event": "subscribe",
        "channel": ["trades"],
        "symbols": ["all"]
    }))


def get_trades(ws, message):
    trade_data = json.loads(message)
    if 'event' in trade_data:
        ws.send(json.dumps({
            "event": 'pong'
        }))
    elif 'data' in trade_data:
        for elem in trade_data['data']:
            lock.acquire()
            print('!', get_unix_time(), elem['symbol'].split('_')[0] + '-' + elem['symbol'].split('_')[1],
                  elem['takerSide'][0].upper(), elem['price'],
                  float(elem['amount']) + float(elem['quantity']), flush=True)
            lock.release()


def sub_order_books_and_deltas(ws):
    for symbol in list_currencies:
        ws.send(json.dumps({
            "event": "subscribe",
            "channel": ["book_lv2"],
            "symbols": [f"{symbol.split('_')[0].lower()}_{symbol.split('_')[1].lower()}"]
        }))


def get_order_books_and_deltas(ws, message):
    if not message:
        return
    order_data = json.loads(message)
    if 'event' in order_data:
        ws.send(json.dumps({
            "event": 'pong'
        }))
    elif 'data' in order_data:
        if order_data['data'][0]['bids']:
            order_answer = '$ ' + str(get_unix_time()) + ' ' + order_data['data'][0]['symbol'].split('_')[0] + '-' + \
                           order_data['data'][0]['symbol'].split('_')[1] + ' B '
            pq = '|'.join(f"{elem[1]}@{elem[0]}"
                          for elem in order_data['data'][0]['bids'])
            lock.acquire()
            if order_data['action'] == "snapshot":
                print(order_answer + pq + ' R', flush=True)
            elif order_data['action'] == "update":
                print(order_answer + pq, flush=True)
            lock.release()

        order_answer = ''
        pq = ''
        if order_data['data'][0]['asks']:
            order_answer = '$ ' + str(get_unix_time()) + ' ' + order_data['data'][0]['symbol'].split('_')[0] + '-' + \
                           order_data['data'][0]['symbol'].split('_')[1] + ' S '
            pq = '|'.join(f"{elem[1]}@{elem[0]}"
                          for elem in order_data['data'][0]['asks'])
            lock.acquire()
            if order_data['action'] == "snapshot":
                print(order_answer + pq + ' R', flush=True)
            elif order_data['action'] == "update":
                print(order_answer + pq, flush=True)
            lock.release()


def main_trades(ws):
    sub_trades(ws)
    while True:
        message_trade = ws.recv()
        try:
            get_trades(ws, message_trade)
        except:
            pass


def main_orders(ws):
    sub_order_books_and_deltas(ws)
    while True:
        message_order = ws.recv()
        time.sleep(0.1)
        try:
            get_order_books_and_deltas(ws, message_order)
        except:
            pass


def main():
    ws = websocket.create_connection('wss://ws.poloniex.com/ws/public')

    t1 = threading.Thread(target=main_trades, args=(ws,))
    t2 = threading.Thread(target=main_orders, args=(ws,))

    #t1.start()
    t2.start()


    # sub_trades(ws)
    # while True:
    #     message_trade = ws.recv()
    #     get_trades(ws, message_trade)

    # sub_order_books_and_deltas(ws)
    # while True:
    #     message_order = ws.recv()
    #     time.sleep(0.1)
    #     get_order_books_and_deltas(ws, message_order)


if __name__ == '__main__':
    main()
