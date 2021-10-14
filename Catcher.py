#
# Python Script with live trading 
# Hermes Strategy Class
# 
# (c) Andres Estrada Cadavid
# QSociety

from Live_Class import Live
import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from os import path, mkdir
from tzlocal import get_localzone
#from kafka import KafkaProducer

class CatcherLive(Live):
    def run_strategy(self, contract_conversion=1, bot_names=[]):
        self.print('%s %s | %s Catcher Bot Turned On' % (self.date, self.hour, self.symbol))
        self.print('%s %s | Running with contract conversion: %.1f'%(self.date, self.hour, contract_conversion))
        # Check if operable schedule
        self.operable_schedule()

        if self.operable:
            self.current_date()

            for kafka_ord in self.consumer:
                self.current_date()
                self.ib.sleep(1)
                
                # Check if it's time to reconnect
                self.connected = self.ib.isConnected()
                self.reconnection()

                if self.connected:
                    kafka_order = eval(str(kafka_ord.value, encoding='utf-8'))
                    #print(kafka_order)

                    ord_type = kafka_order['type']

                    # Entry Braket Market
                    if ord_type == 'BKT_MKT':
                        self.print('modification: %s'%ord_type)
                        bot_name = kafka_order['bot_name']
                        local_id = kafka_order['local_id']
                        action = kafka_order['action']
                        qty = kafka_order['qty']*contract_conversion
                        sl_price = kafka_order['sl_price']
                        tp_price = kafka_order['tp_price']
                        max_stop = kafka_order['max_stop']
                        price_in, sl, tp, time_in, comm_in, profit, ord_sl, ord_tp = self.braket_market(bot_name, action, qty, sl_price, 
                                                tp_price, max_stop, send_message=False, prints=False, save_image=False, send_image=False)

                        local_order = pd.DataFrame({'local_id':local_id, 'qty':qty, 'action':action, 'price_in':price_in, 'sl':sl, 'tp':tp, 
                                        'time_in':time_in, 'comm_in':comm_in, 'profit':profit, 'ord_sl':ord_sl, 'ord_tp':ord_tp}, index=[0])

                        self.local_orders = pd.concat([self.local_orders, local_order])
                    
                    # Trailing Stop
                    elif ord_type == 'TRA_STP':
                        self.print('modification: %s'%ord_type)
                        local_id = kafka_order['local_id']
                        new_sl_price = kafka_order['new_sl_pr']
                        order = self.local_orders.ord_sl.loc[self.local_orders.local_id == local_id][0]

                        order.auxPrice = new_sl_price
                        self.ib.placeOrder(self.contract, order)

                    # Exit Pending
                    elif ord_type == 'EXT_PEN':
                        self.print('modification: %s'%ord_type)
                        bot_name = kafka_order['bot_name']
                        local_id = kafka_order['local_id']
                        ord_close = kafka_order['ord_close']
                        order_df = self.local_orders.loc[self.local_orders.local_id == local_id]
                        order_to_close = order_df['ord_%s'%ord_close][0]
                        in_action = order_df.action[0]
                        qty = order_df.qty[0]
                        price_in = order_df.price_in[0]
                        time_in = order_df.time_in[0]
                        comm_in = order_df.comm_in[0]
                        comment = kafka_order['comment']

                        self.exit_pending(bot_name, order_to_close, in_action, qty, price_in, time_in, comm_in, comment)

                    # Exit Market
                    elif ord_type == 'EXT_MKT':
                        self.print('modification: %s'%ord_type)
                        bot_name = kafka_order['bot_name']
                        local_id = kafka_order['local_id']
                        ord_cancel = kafka_order['ord_cancel']
                        order_df = self.local_orders.loc[self.local_orders.local_id == local_id]
                        order_to_cancel = order_df['ord_%s'%ord_cancel][0]
                        in_action = order_df.action[0]
                        qty = order_df.qty[0]
                        price_in = order_df.price_in[0]
                        time_in = order_df.time_in[0]
                        comm_in = order_df.comm_in[0]
                        comment = kafka_order['comment']

                        self.exit_market(bot_name, order_to_cancel, in_action, qty, price_in, time_in, comm_in, comment)
                    
                    else:
                        self.continuous_check_message('%s %s | %s %s is running OK' % (self.date, self.hour, self.bot_name, self.symbol)))
                        self.daily_results_positions(bot_names)         # Send daily profit message to telegram
                        self.weekly_metrics(bot_names)                  # Send week metrics message to telegram

                else:
                    if not self.interrumption:
                        try:
                            self.print('Trying to reconnect...')
                            self.ib.disconnect()
                            self.ib.sleep(10)
                            self.ib.connect('127.0.0.1', self.port, self.client)
                            self.connected = self.ib.isConnected()
                            if self.connected:
                                self.print('Connection reestablished!')
                        except:
                            self.print('Connection Failed! Trying to reconnect in 10 seconds...')

            self.ib.disconnect()
            self.print('%s %s | Session Ended. Good Bye!' % (self.date, self.hour))

if __name__ == '__main__':
    symbol = 'MNQ'
    port = 7497
    client = 70

    live_hermes = LiveHermes(symbol=symbol, bot_name='Catcher', temp='1 min', port=port, client=client, real=True)
    live_hermes.run_strategy(contract_conversion=1, bot_names=['Hermes Longs Rev', 'Hermes Shorts', 
                                                                'Nene Breakout Longs', 'Nene Breakout Shorts'])