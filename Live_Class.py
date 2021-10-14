#
# Python Script with live trading Class
#
# (c) Andres Estrada Cadavid
# QSociety

import pandas as pd
from datetime import datetime, timedelta
import requests
from ib_insync import IB, Future, Forex, Stock, util, MarketOrder, StopOrder, LimitOrder
from nested_lookup import nested_lookup
from platform import system
from os import path, mkdir
from tzlocal import get_localzone
import numpy as np
import matplotlib.pyplot as plt
from math import sqrt
from kafka import KafkaConsumer
from random_word import RandomWords

class Live():
    def __init__(self, symbol, bot_name, temp, port, client, real=False, transmit=False):
        self.symbol = symbol
        self.bot_name = bot_name
        self.temp = temp
        self.real = real
        self.transmit = transmit
        instruments = pd.read_csv('instruments.csv').set_index('symbol')
        self.parameters = instruments.loc[self.symbol]
        self.market = str(self.parameters.market)
        self.exchange = str(self.parameters.exchange)
        self.tick_size = float(self.parameters.tick_size)
        self.digits = int(self.parameters.digits)
        self.leverage = int(self.parameters.leverage)
        self.port = port
        self.client = client
        self.current_date()
        self.print('Trying to Connect to Trading Platform...', 'w')
        self._sundays_activation()
        self.ib = IB()
        self.print(self.ib.connect('127.0.0.1', port, self.client))
        self.connected = self.ib.isConnected()
        self._get_contract()
        self.close_opened_position()
        self.cancel_pending_orders()
        self.interrumption = False
        #self.consumer = KafkaConsumer('%s_order'%self.symbol, bootstrap_servers='kafka1:19092')
        self.consumer = KafkaConsumer('%s_order'%self.symbol, bootstrap_servers='157.245.223.103:9092')
        # Variables
        self.random = RandomWords()
        self.cont_mess = False
        self.send_weekly = False
        self.send_daily = False
        self.current_len = 0
        self.last_len = 0
        self.position = 0
        self.operable = True
        self.local_id = 0
        self.local_orders = pd.DataFrame(columns=['local_id', 'qty', 'action', 'price_in', 'sl', 'tp', 'time_in', 'comm_in', 'profit', 
                                                'ord_sl', 'ord_tp'])
        self.initialize_csv()
        self.total_executions = pd.read_csv('trades/total_executions_%s.csv'%self.symbol)
        self.total_trades = pd.read_csv('trades/total_trades_%s.csv'%self.symbol)
        self.opened = pd.DataFrame(columns=['instrument','action','qty','price','time','commission'])
    
    def print(self, message, type='a'):
        sample = open('%s_logs.txt'%self.symbol, type)
        print(message, file=sample)
        print(message)
        sample.close()

    def x_round(self, x):
        '''Round price values according to tick size

        Parameters:
            x (float): number to be rounded
        
        Returns:
            float: rounded number
        '''
        mult = 1/self.tick_size
        return round(x*mult)/mult

    def initialize_csv(self):
        '''Create CSV files when they don't exist
        '''
        if not path.exists('trades/total_executions_%s.csv'%self.symbol):
            mkdir('trades')
            initial_executions = pd.DataFrame(columns=['instrument', 'bot_name', 'action','qty','price','time','commission','local_id', 'order_id'])
            initial_executions.to_csv('trades/total_executions_%s.csv'%self.symbol, index=False)
        if not path.exists('trades/total_trades_%s.csv'%self.symbol):
            initial_trades = pd.DataFrame(columns=['instrument', 'bot_name', 'market_position','qty','entry_price','exit_price','entry_time',
                                                    'exit_time','profit','cum_net_profit','commission','comment'])
            initial_trades.to_csv('trades/total_trades_%s.csv'%self.symbol, index=False)
        if not path.exists('%s_trades_images' % self.symbol):
            mkdir('%s_trades_images' % self.symbol)
        if not path.exists('%s_entry_images' % self.symbol):
            mkdir('%s_entry_images' % self.symbol)

    def current_date(self):
        '''Get current date, weekday and hour
        '''
        self.date = datetime.now().strftime('%Y-%m-%d')
        self.weekday = datetime.now().weekday()
        self.hour = datetime.now().strftime('%H:%M:%S')
    
    def continuous_check_message(self, message):
        '''Every hour message confirmation

        Parameters:
            message (str): Message to be sent
        '''
        #if datetime.now().minute == 0 and datetime.now().second == 0:
        if datetime.now().minute == 0 and not self.cont_mess:
            self.send_telegram_message(message, type='info')
            self.cont_mess = True
            #if not datetime.now().hour == 18:
                #self.check_data_farm()
        if datetime.now().minute != 0:
            self.cont_mess = False

    def _sundays_activation(self):
        '''Sundays bot Activation when market opens
        '''
        hour = '18:00:05'
        if self.weekday == 6:
            if pd.to_datetime(self.hour).time() < pd.to_datetime(hour).time():
                self.print('Today is Sunday. Bot activation is at 18:00:00')
                while True:
                    self.current_date()
                    if pd.to_datetime(self.hour).time() >= pd.to_datetime(hour).time():
                        self.print('Activation Done')
                        self.send_telegram_message('%s %s | Bot Activation Done. %s %s'%(self.date, self.hour, self.bot_name, self.symbol))
                        break
    
    def operable_schedule(self):
        '''Defines operable schedules
        '''
        if self.weekday == 4 and pd.to_datetime(self.hour).time() > pd.to_datetime('18:00:00').time():
            self.print('%s %s | Today is Friday and Market has Closed!'%(self.date, self.hour))
            self.operable = False
        elif self.weekday == 5:
            self.print('%s %s | Today is Saturday and market is not Opened'%(self.date, self.hour))
            self.operable = False
        else:
            self.operable = True

    def _local_symbol_selection(self):
        '''Selects local symbol according to symbol and current date

        Returns:
            str:local symbol according to symbol and current date
        '''
        current_date = datetime.now().date()
        if self.symbol in ['ES', 'RTY', 'NQ', 'MES', 'MNQ', 'M2K']:
            contract_dates = pd.read_csv('contract_dates/indexes_globex.txt', parse_dates=True)
        elif self.symbol in ['YM', 'MYM', 'DAX']:
            contract_dates = pd.read_csv('contract_dates/indexes_ecbot_dtb.txt', parse_dates=True)
        elif self.symbol in ['QO', 'MGC']:
            contract_dates = pd.read_csv('contract_dates/QO_MGC.txt', parse_dates=True)
        elif self.symbol in ['CL', 'QM']: 
            contract_dates = pd.read_csv('contract_dates/CL_QM.txt', parse_dates=True)
        else: 
            contract_dates = pd.read_csv('contract_dates/%s.txt'%self.symbol, parse_dates=True)

        # Current contract selection according to current date
        for i in range(len(contract_dates)):
            initial_date = pd.to_datetime(contract_dates.iloc[i].initial_date).date()
            final_date = pd.to_datetime(contract_dates.iloc[i].final_date).date()
            if initial_date <= current_date <= final_date:
                current_contract = contract_dates.iloc[i].contract
                break
        
        # local symbol selection
        local = current_contract
        if self.symbol in ['ES', 'RTY', 'NQ', 'MES', 'MNQ', 'M2K', 'QO', 'CL', 'MGC', 'QM']:
            local = '%s%s'%(self.symbol, current_contract)
        if self.symbol in ['YM', 'ZS']: local = '%s   %s'%(self.symbol, current_contract)
        if self.symbol == 'MYM': local = '%s  %s'%(self.symbol, current_contract)
        if self.symbol == 'DAX': local = 'FDAX %s'%current_contract
        
        return local

    def _get_contract(self):
        '''Get current contract given symbol and current date
        '''
        if self.market == 'futures':
            local = self._local_symbol_selection()
            self.contract = Future(symbol=self.symbol, exchange=self.exchange, localSymbol=local)
        elif self.market == 'forex':
            self.contract = Forex(self.symbol)
        elif self.market == 'stocks':
            self.contract = Stock(symbol=self.symbol, exchange=self.exchange, currency='USD')

    def daily_results_positions(self, bot_names):
        '''Send telegram message with daily profit and opened positions at 5 pm
        '''
        if pd.to_datetime('16:58:00').time() <= pd.to_datetime(self.hour).time() <= pd.to_datetime('16:59:59').time():
            if not self.send_daily:
                trades = self.total_trades.copy(True)
                for name in bot_names:
                    day_profit = 0
                    bot_trades = trades.loc[trades.bot_name==name]
                    if len(bot_trades) > 0:
                        try:
                            #trades.reset_index(inplace=True)
                            bot_trades.set_index('exit_time', inplace=True)
                            bot_trades.index = pd.to_datetime(bot_trades.index)
                            init_date = '%s 18:00:00'%str(datetime.now().date()-timedelta(days=1))
                            final_date = '%s 17:00:00'%str(datetime.now().date())
                            day_profit = bot_trades.loc[init_date:final_date].profit.sum()
                        except: day_profit = 0
                    message = '='*30+'\n' + name
                    message += "\nToday's profit in %s was %.2f USD\n"%(self.symbol, day_profit)
                    total = '%d Long(s)'%self.position if self.position > 0 else '%d Short(s)'%abs(self.position) if self.position < 0 else 'None'
                    message += 'Opened Positions in %s: %s\n'%(self.symbol, total) + '='*30
                    self.send_telegram_message(message, type='info')
                self.send_daily = True
        else: self.send_daily =False

    def weekly_metrics(self, bot_names):
        '''Send Telegram message with week trades metrics
        '''
        if self.weekday == 4 and pd.to_datetime(self.hour).time() >= pd.to_datetime('16:59:00').time() and not self.send_weekly:
            trades = self.total_trades.copy(True)
            for name in bot_names:
                bot_trades = trades.loc[trades.bot_name==name]
                if len(bot_trades) > 0:
                    bot_trades.set_index('exit_time', inplace=True)
                    bot_trades.index = pd.to_datetime(bot_trades.index)
                    monday_date = str(pd.to_datetime(self.date).date() - timedelta(days=4))
                    week_trades = bot_trades.loc[monday_date:self.date].copy(True)
                    if len(week_trades):
                        self.calculate_metrics(week_trades)
                        message = '='*30 + '\nWEEK METRICS %s (%s - %s)'%(name,monday_date,self.date) + '\nTotal Trades: %d'%self.trades_total
                        message += '\nTotal Profit: %.2f USD'%self.total_profit + '\nNet Profit: %.2f USD'%self.net_profit
                        message += '\nMax. Drawdown: %.2f USD'%self.max_drawdown + '\nPercent Profitable: %.2f %%'%self.percent_profitable
                        message += '\nProfit Factor: %.2f'%self.profit_factor + '\nSharpe Ratio: %.2f'%self.sharpe_ratio + '\nSQN: %.2f'%self.sqn +'\n'
                        message += '='*30
                    else:
                        monday_date = str(pd.to_datetime(self.date).date() - timedelta(days=4))
                        message = '='*30 + '\nWEEK METRICS %s (%s - %s)'%(name,monday_date,self.date) +'\n     No Trades this Week\n' + '='*30
                else:
                    monday_date = str(pd.to_datetime(self.date).date() - timedelta(days=4))
                    message = '='*30 + '\nWEEK METRICS %s (%s - %s)'%(name,monday_date,self.date) +'\n     No Trades this Week\n' + '='*30
                self.send_telegram_message(message, type='info')
            self.send_weekly = True
            #self.send_telegram_message(message, type='inv')

    def calculate_metrics(self, trades):
        '''Calculate metrics os given trades

        Parameters:
            trades (DataFrame): trades to calculate metrics
        '''
        trades['net_profit'] = trades['profit'] - trades['commission']
        trades['accumulated_profit'] = trades['net_profit'].cumsum()
        trades['max_profit'] = trades['accumulated_profit'].cummax()
        self.total_profit = round((trades['profit']).sum(),2)
        self.total_commissions = round(trades['commission'].sum(),2)
        self.net_profit = self.total_profit - self.total_commissions
        self.gross_profit = round(trades[trades['profit'] > 0]['profit'].sum(), 2)
        self.gross_loss = round(trades[trades['profit'] <= 0]['profit'].sum(), 2)
        self.profit_factor = (lambda prof,loss:abs(prof/loss) if loss != 0 else 0)(self.gross_profit,self.gross_loss)
        self.max_drawdown = (trades['max_profit'] - trades['accumulated_profit']).max()
        self.trades_total = len(trades)
        self.total_positive = len(trades[trades['profit']>0])
        self.percent_profitable = self.total_positive*100/self.trades_total
        self.sharpe_ratio = (trades['profit'].mean() * len(trades)) / (trades['profit'].std() * sqrt(len(trades)))
        self.sqn = sqrt(len(trades))*(trades.net_profit.mean()/trades.net_profit.std())

    def reconnection(self):
        '''Disconnection and reconnection in platform and market closing
        '''
        now = datetime.now()
        hour = now.hour
        minute = now.minute
        second = now.second
        if (hour == 23 and minute == 44 and (second == 30 or second == 31 or second == 32)) or \
            (hour == 16 and minute == 59 and (second == 30 or second == 31 or second == 32)):
            self.interrumption = True
            self.ib.disconnect()
            self.connected = self.ib.isConnected()
            self.print('%s %s | Ib disconnection' % (self.date, self.hour))
            self.print('Connected: %s' % self.connected)
            self.ib.sleep(3)
        if (hour == 23 and minute == 46 and (second == 0 or second == 1 or second == 2)) or \
            (hour == 18 and minute == 0 and (second == 5 or second == 6 or second == 7)):
        #if self.hour == '23:46:00' or self.hour == '18:00:05':
            self.interrumption = False
            self.print('%s %s | Reconnecting...' % (self.date, self.hour))
            while not self.connected:
                try:
                    self.ib.connect('127.0.0.1',self.port, self.client)
                    self.connected = self.ib.isConnected()
                    if self.connected:
                        self.print('%s %s | Connection reestablished!' % (self.date, self.hour))
                        #self.print('Requesting Market Data...')
                        #self.bars = self.ib.reqRealTimeBars(self.contract, 5, 'TRADES', False)
                        #self.print('Last Close of %s: %.2f' % (self.symbol, self.bars[-1].close))
                        #self.print('%s Data has been Updated!' % self.symbol)
                except:
                    self.print('%s %s | Connection Failed! Trying to reconnect in 10 seconds...' % (self.date, self.hour))
                    self.ib.sleep(10)
            self.print('%s %s | %s Data has been Updated!' % (self.date, self.hour, self.symbol))
            self.ib.sleep(3)

    def _get_values(self, order_id):
        '''Get price and qty (number of lots or contracts) given an order id

        Parameters:
            order_id (int): order id 
        
        Returns:
            list: list of filled prices
            list: list of lots or contracts filled
        '''
        price = 0; prices = []
        qty = 0; qtys = []
        for trade in util.tree(self.ib.fills()):
            val = 1 if system()=='Windows' else 'execution'
            if 'orderId' in trade[val]['Execution']:
                if ((nested_lookup('orderId',trade)[0] == order_id) and (nested_lookup('symbol',trade)[0] == self.symbol)):
                    price = nested_lookup('price', trade)[0]
                    qty = int(nested_lookup('shares', trade)[0])
                    prices.append(price)
                    qtys.append(qty)
        return prices, qtys

    def _save_execution(self, bot_name, operation, order_type='market'):
        execution = []
        iterator = util.tree(operation)['Trade']['fills'] if order_type == 'market' else operation
        for trade in iterator:
            symbol = nested_lookup('symbol', trade)[0]
            if symbol == self.symbol:
                action = 'SELL' if nested_lookup('side', trade)[0] == 'SLD' else 'BUY'
                qty = int(nested_lookup('shares', trade)[0])
                price = nested_lookup('price', trade)[0]
                #time = pd.to_datetime(nested_lookup('time', trade)[0]).tz_convert('US/Eastern').tz_localize(None)
                time = pd.to_datetime('%s %s'%(self.date, self.hour)).tz_localize(str(get_localzone())).tz_convert('US/Eastern').tz_localize(None)
                try: commission = nested_lookup('commission', trade)[0]
                except: commission = self.parameters.comm_value
                order_id = int(nested_lookup('orderId', trade)[0])
                for _ in range(int(qty)):
                    self.local_id += 1
                    execu = {'instrument':symbol, 'bot_name':bot_name, 'action':action, 'qty':1, 'price':price, 'time':time, 'commission':commission,
                        'order_id':order_id, 'local_id':self.local_id}
                    execution.append(execu)
        return execution
    
    def _save_trade(self, bot_name, order, order_type='market', comment=''):
        '''Adjust opened orders

        Parameters:
            order_id (int): order id
            action (str): order direction ('BUY' or 'SELL')
        
        Returns:
            float: sum of total profit of a trade
            float: mean price filled
            float: total commissions
        '''
        execution = self._save_execution(bot_name, order, order_type)
        execution = pd.DataFrame(execution, index=[0]) if len(execution)==1 else pd.DataFrame(execution)
        self.total_executions = pd.concat([self.total_executions, execution], sort=False)
        self.total_executions.reset_index(drop=True, inplace=True)
        self.total_executions.to_csv('trades/total_executions_%s.csv'%self.symbol, index=False)
        self.opened = pd.concat([self.opened, execution], sort=False)
        self.opened.reset_index(drop=True, inplace=True)
        
        profits = []
        if len(np.unique(self.opened.action)) > 1:
            entry_act = self.opened.action.iloc[0]
            exit_act =  'SELL' if entry_act=='BUY' else 'BUY'
            entry_operations = self.opened[self.opened.action == entry_act]
            exit_operations = self.opened[self.opened.action == exit_act]
            num_calcing = min(len(entry_operations), len(exit_operations))
            for i in range(num_calcing):
                entry_ex = entry_operations.iloc[i]; exit_ex = exit_operations.iloc[i]
                entry_id = entry_ex.local_id; exit_id = exit_ex.local_id
                action = entry_ex.action; qty = entry_ex.qty
                entry_price = entry_ex.price; exit_price = exit_ex.price
                commission = entry_ex.commission + exit_ex.commission
                profit = (1 if action=='BUY' else -1)*(exit_price-entry_price)*self.leverage*qty
                cum_profit = self.total_trades.profit.sum() + profit
                trade = {'instrument':entry_ex.instrument, 'bot_name':entry_ex.bot_name, 'market_position':action, 'qty':qty, 
                        'entry_price': entry_ex.price, 'exit_price':exit_ex.price, 'entry_time':entry_ex.time,
                        'exit_time':exit_ex.time, 'profit':profit, 'cum_net_profit':cum_profit, 
                        'commission':commission, 'comment':comment}
                self.total_trades = pd.concat([self.total_trades, pd.DataFrame(trade, index=[0])], sort=False)
                self.total_trades.reset_index(drop=True, inplace=True)
                self.total_trades.to_csv('trades/total_trades_%s.csv'%self.symbol, index=False)
                self.opened = self.opened[(self.opened.local_id != entry_id) & (self.opened.local_id != exit_id)]
                self.opened.reset_index(drop=True, inplace=True)
                profits.append(profit)
        
        return (sum(profits), np.mean(execution.price), np.sum(execution.commission))

    def market_order(self, bot_name, action, qty, comment=''):
        '''Open Market Order

        Parameters:
            action (str): order direction ('BUY' or 'SELL')
            qty (int): lots or contracts quantity
        
        Returns:
            float: price filled
            float: total commission
            float: total profit
        '''
        market_order = MarketOrder(action, int(qty))
        operation = self.ib.placeOrder(self.contract, market_order)
        self.ib.sleep(2*int(qty))
        profit, price, commission = self._save_trade(bot_name=bot_name, order=operation, comment=comment)
        return (price, commission, profit)

    def stop_order(self, action, qty, price):
        '''Send Stop Order

        Parameters:
            action (str): order direction ('BUY' or 'SELL')
            qty (int): lots or contracts quantity
            price (float): stop order price
        
        Return:
            object: stop order
        '''
        stop_ord = StopOrder(action, int(qty), price, tif='GTC', outsideRth=True)
        self.ib.placeOrder(self.contract, stop_ord)
        return (stop_ord)
    
    def limit_order(self, action, qty, price):
        '''Send Limit Order

        Parameters:
            action (str): order direction ('BUY' or 'SELL')
            qty (int): lots or contracts quantity
            price (float): limit order price
        
        Return:
            object: limit order
        '''
        limit_ord = LimitOrder(action, int(qty), price, tif='GTC', outsideRth=True)
        self.ib.placeOrder(self.contract, limit_ord)
        return (limit_ord)

    def braket_market(self, bot_name, action, qty, sl_price, tp_price, max_stop, entry_price=0, send_message=True, prints=True, save_image=True, send_image=True):
        price_in = 0; ord_sl = []; ord_tp = []
        allow_margin = self.check_margins(bot_name, qty, action, max_stop=max_stop)
        if allow_margin:
            price_in, comm_in, profit = self.market_order(action, qty)
            price_in = self.x_round(price_in)
            if not price_in > 0:
                self.close_opened_position(); self.cancel_pending_orders()
            else:
                try:
                    time_in = str(pd.to_datetime('%s %s'%(self.date, self.hour)).tz_localize(str(get_localzone())).tz_convert('US/Eastern').tz_localize(None))
                    mult = 1 if action=='BUY' else -1
                    contrary_action = 'BUY' if action == 'SELL' else 'SELL'
                    ord_tp = LimitOrder(contrary_action, qty, tp_price, tif='GTC', outsideRth=True)
                    ord_sl = StopOrder(contrary_action, qty, sl_price, tif='GTC', outsideRth=True)
                    group_id = '%s_%s'%(self.random.get_random_word(),self.random.get_random_word())
                    self.ib.oneCancelsAll([ord_sl, ord_tp], group_id, 1); print(group_id)
                    self.ib.placeOrder(self.contract, ord_sl); self.ib.sleep(0.5)
                    self.ib.placeOrder(self.contract, ord_tp)
                    self.position += mult*int(qty); self.save_position()
                    if send_message: self.send_message_in(bot_name=bot_name, action=action, price_in=price_in, sl=sl_price, tp=tp_price, qty=int(qty), type='trades')
                    if prints: self.print('%s %s | %s Opended of %d units at %.2f in %s' % (self.date, self.hour, action, int(qty), price_in, self.symbol))
                    if save_image:
                        try:
                            # 15 Minutes Image
                            self.entry_image(self.data, action, price_in, time_in, sl_price, tp_price)
                            if send_image:
                                image_name = '%s_entry_images/%s at %s(%.2f sl %.2f tp %.2f) in %s.png'%(self.symbol, action, time_in.replace(':','.'), price_in, sl_price, tp_price, self.symbol)
                                self.send_telegram_image(image_name)
                        except: self.send_telegram_message('No entry image available!')
                except:
                    self.send_telegram_message('Order could not be sent. Please check!')
                    self.ib.disconnect(); self.ib.sleep(1.5)
                    self.ib.connect('127.0.0.1', self.port, self.client)
                    self.close_opened_position(); self.cancel_pending_orders()
                    self.position = 0; self.save_position()
        else: 
            self.close_opened_position(); self.cancel_pending_orders()
            self.ib.disconnect()
            self.send_telegram_message('%s %s\nThere is not enough Margin'%(bot_name, self.symbol))
        return price_in, sl_price, tp_price, time_in, comm_in, profit, ord_sl, ord_tp

    def exit_pending(self, bot_name, order_to_close, in_action, qty, price_in, time_in, comm_in, comment, send_message=True, prints=True, save_image=True, send_image=True):
        mult = 1 if in_action == 'SELL' else -1
        self.position += mult*int(qty)
        self.save_position()
        price_out, comm_out, profit = self.check_pendings(bot_name, order_to_close, what='values', comment=comment)
        time_out = str(pd.to_datetime('%s %s'%(self.date, self.hour)).tz_localize(str(get_localzone())).tz_convert('US/Eastern').tz_localize(None))
        if send_message: self.send_message_out(bot_name=bot_name, action=in_action, price_out=price_out, qty=int(qty), profit=profit, comm_in=comm_in, comm_out=comm_out, comment=comment)
        if prints: self.print('%s %s | %s Closed of %d units at %.2f in %s and profit %.2f (%s)' % (self.date, self.hour, in_action, int(qty), price_out, self.symbol, profit, comment))
        if save_image:
            try:
                # 15 Minutes image
                self.trade_image(self.data, in_action, price_in, price_out, time_in, time_out)
                if send_image:
                    image_name = '%s_trades_images/%s at %s(%.2f) %s(%.2f) in %s.png'%(self.symbol, in_action, time_in.replace(':','.'), price_in, time_out.replace(':','.'), price_out, self.symbol)
                    self.send_telegram_image(image_name)
            except: self.send_telegram_message('No exit image available!')
    
    def exit_market(self, bot_name, order_to_cancel, in_action, qty, price_in, time_in, comm_in, comment, send_message=True, prints=True, save_image=True, send_image=True):
        self.ib.cancelOrder(order_to_cancel)
        exit_action = 'BUY' if in_action == 'SELL' else 'SELL'
        price_out, comm_out, profit = self.market_order(exit_action, qty, comment=comment)
        time_out = str(pd.to_datetime('%s %s'%(self.date, self.hour)).tz_localize(str(get_localzone())).tz_convert('US/Eastern').tz_localize(None))
        mult = 1 if in_action == 'SELL' else -1
        self.position += mult*int(qty)
        self.save_position()
        if send_message: self.send_message_out(bot_name=bot_name, action=in_action, price_out=price_out, qty=int(qty), profit=profit, comm_in=comm_in, comm_out=comm_out, comment=comment)
        if prints: self.print('%s %s | %s Closed of %d units at %.2f in %s and profit %.2f (%s)' % (self.date, self.hour, in_action, int(qty), price_out, self.symbol, profit, comment))
        if save_image:
            try:
                # 15 Minutes image
                self.trade_image(self.data, in_action, price_in, price_out, time_in, time_out)
                if send_image:
                    image_name = '%s_trades_images/%s at %s(%.2f) %s(%.2f) in %s.png'%(self.symbol, in_action, time_in.replace(':','.'), price_in, time_out.replace(':','.'), price_out, self.symbol)
                    self.send_telegram_image(image_name)
            except: self.send_telegram_message('No exit image available!')
    
    def check_pendings(self, bot_name, order, what='fill', comment=''):
        '''Check if a pending order is filled or not

        Parameters:
            order (object): order
            what (str): what to return. 'fill' returns boolean. 'values' returns order values
        
        Returns:
            bool: True or False if order is filled
            float: price filled
            float: total commission
            float: total profit
        '''
        filled = False
        order_id = order.orderId
        perm_id = order.permId
        for trade in (util.tree(self.ib.fills())):
            if [order_id]==nested_lookup('orderId',trade) and [self.symbol]==nested_lookup('symbol',trade) and [self.client]==nested_lookup('clientId',trade) and [perm_id]==nested_lookup('permId',trade):
                filled = True
        if what == 'values' and filled:
            self.ib.sleep(1.5)
            operation = []
            for order in util.tree(self.ib.fills()):
                if nested_lookup('orderId',order)==[order_id] and nested_lookup('symbol',order)==[self.symbol] and nested_lookup('clientId',order)==[self.client] and nested_lookup('permId',order)==[perm_id]:
                    operation.append(order)

        if what == 'fill':
            return filled
        else: 
            profit, price, commission = self._save_trade(bot_name=bot_name, order=operation, order_type='pending', comment=comment)
            return(price, commission, profit)

    def close_opened_position(self):
        '''Close opened positions in the current symbol
        '''
        opened_position = 0
        val = 2 if system()=='Windows' else 'position'
        if len(util.tree(self.ib.positions())) > 0:
            for position in util.tree(self.ib.positions()):
                if nested_lookup('symbol', position)[0] == self.symbol and nested_lookup('secType', position)[0] == 'FUT':
                    opened_position = int(position[val])
        
            if opened_position != 0:
                self.print('opened positions: %d' % opened_position)
                close_action = 'SELL' if opened_position > 0 else 'BUY'
                market_order = MarketOrder(close_action, abs(opened_position))
                self.ib.placeOrder(self.contract, market_order)
                self.ib.sleep(2*abs(opened_position))
                prices, qtys = self._get_values(order_id=market_order.orderId)
                self.print('opened positions closed')
                action = 'SELL' if close_action == 'BUY' else 'BUY'
                message = 'Opened %s in %s was closed\n'%(action, self.symbol)
                message += 'Price: %.2f\nContracts: %d'%(np.mean(prices), sum(qtys))
                self.send_telegram_message(message)#; self.send_telegram_message(message, type='inv')
            else: self.print('No opened positions in %s'%self.symbol)
        else: self.print('No opened positions')

    def cancel_pending_orders(self):
        '''Cancel submitted pending orders
        '''
        if len(util.tree(self.ib.trades())) > 0:
            for order in util.tree(self.ib.trades()):
                if nested_lookup('symbol', order)[0] == self.symbol:
                    if nested_lookup('status', order)[0] in ['PreSubmitted', 'Submitted'] and nested_lookup('orderType', order)[0] in ['STP', 'LMT']:
                        ord_id = nested_lookup('orderId', order)[0]
                        ord_type = nested_lookup('orderType', order)[0]
                        self.ib.client.cancelOrder(ord_id)
                        self.print('%s order with id %d is cancelled' % (ord_type, ord_id))
    
    '''def send_telegram_message(self, message, bot_name='', type='trades'): #
        ''Send telegram message to an specific group

        Parameters:
            message (string): Message to be sent
            type (string): if 'trades' sends message to trades telegram group. if 'info' sends message to information telegram group
        ''
        telegram_credentials = pd.read_csv('telegram_credentials.csv')

        bot_token = telegram_credentials['bot_token'].iloc[0]
        chatID_info = telegram_credentials['bot_chatID_info'].iloc[0]
        chatID_alarm = telegram_credentials['bot_chatID_alarm'].iloc[0]
        if type == 'trades':
            name = bot_name.split()[0].lower()
            bot_id = '%s_real'%name if self.real else '%s_demo'%name
            bot_chatID = telegram_credentials['bot_chatID_%s'%bot_id].iloc[0]
        else: bot_chatID = chatID_alarm if type=='alarm' else chatID_info
        #bot_chatID = chatID_trades if type=='trades' else chatID_inv if type=='inv' else chatID_alarm if type=='alarm' else chatID_info
        url = 'https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s'%(bot_token, bot_chatID, message)
    
        requests.get(url)'''

    def send_telegram_message(self, message, type='trades'):
        '''Send telegram message to an specific group

        Parameters:
            message (string): Message to be sent
            type (string): if 'trades' sends message to trades telegram group. if 'info' sends message to information telegram group
        '''
        telegram_credentials = pd.read_csv('telegram_credentials_.csv')
        bot_token = telegram_credentials['bot_token'].iloc[0]
        chatID_info = telegram_credentials['bot_chatID_info'].iloc[0]
        if type == 'trades':
            bot_chatID = telegram_credentials['bot_chatID_%s'%self.symbol].iloc[0]
        else: bot_chatID = chatID_info
        url = 'https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s'%(bot_token, bot_chatID, message)
    
        requests.get(url)

    '''def send_telegram_image(self, bot_name, filename, type='trades'):
        ''Send telegram image to an specific group

        Parameters:
            filename (string): image name to be sent
            type (string): if 'trades' sends message to trades telegram group. if 'info' sends message to information telegram group
        ''
        telegram_credentials = pd.read_csv('telegram_credentials.csv')

        bot_token = telegram_credentials['bot_token'].iloc[0]
        chatID_info = telegram_credentials['bot_chatID_info'].iloc[0]
        chatID_alarm = telegram_credentials['bot_chatID_alarm'].iloc[0]
        if type == 'trades':
            name = bot_name.split()[0].lower()
            bot_id = '%s_real'%name if self.real else '%s_demo'%name
            bot_chatID = telegram_credentials['bot_chatID_%s'%bot_id].iloc[0]
        else: bot_chatID = chatID_alarm if type=='alarm' else chatID_info
        files = {'photo':open(filename, 'rb')}
        url = 'https://api.telegram.org/bot%s/sendPhoto?chat_id=%s'%(bot_token, bot_chatID)
    
        requests.post(url, files=files)'''

    def send_telegram_image(self, filename, type='trades'):
        '''Send telegram image to an specific group

        Parameters:
            filename (string): image name to be sent
            type (string): if 'trades' sends message to trades telegram group. if 'info' sends message to information telegram group
        '''
        telegram_credentials = pd.read_csv('telegram_credentials_.csv')

        bot_token = telegram_credentials['bot_token'].iloc[0]
        chatID_info = telegram_credentials['bot_chatID_info'].iloc[0]
        if type == 'trades':
            bot_chatID = telegram_credentials['bot_chatID_%s'%self.symbol].iloc[0]
        else: bot_chatID = chatID_info
        files = {'photo':open(filename, 'rb')}
        url = 'https://api.telegram.org/bot%s/sendPhoto?chat_id=%s'%(bot_token, bot_chatID)
    
        requests.post(url, files=files)

    def send_message_in(self, bot_name, action, price_in, qty, sl=0, tp=0, type='trades'):
        '''Send telegram message when a position is opened

        Parameters:
            action (string): order direction ('BUY' or 'SELL')
            price_in (float): entry price
            sl (float): Stop Loss price level
            tp (sloat): Take Profit price level
            qty (int): lots or contracts quantity
        '''
        if type == 'inv':
            msg_in = '%s \n%s Opened in %s \nPrice: %5.2f \nContracts: %d \nAt: %s' % (bot_name, action, self.symbol, price_in, qty, self.hour)
        else:
            msg_in = '%s \n%s Opened in %s \nPrice: %5.2f \nS/L: %5.2f \nT/P: %5.2f \nContracts: %d \nAt: %s' % (bot_name, action, self.symbol, price_in, sl, tp, qty, self.hour)
        self.send_telegram_message(msg_in, type=type)
    
    def send_message_out(self, bot_name, action, price_out, qty, profit, comm_in, comm_out, comment='', type='trades'):
        '''Send telegram message when a position is closed

        Parameters:
            action (string): order direction ('BUY' or 'SELL')
            price_out (float): exit price
            qty (int): lots or contracts quantity
            profit (float): total profit
            comm_in (float): entry commission
            comm_out (float): exit commission
        '''
        if type == 'trades':
            msg_out = '%s \n%s Closed in %s (%s) \nPrice: %5.2f \nContracts: %d \nProfit(USD): %5.2f \nCommissions(USD): %5.2f \nAt: %s' % \
                    (bot_name, action, self.symbol, comment, price_out, qty, profit, (comm_in+comm_out),self.hour)
        else:
            msg_out = '%s \n%s Closed in %s \nPrice: %5.2f \nContracts: %d \nProfit(USD): %5.2f \nAt: %s' % \
                    (bot_name, action, self.symbol, price_out, qty, (profit-comm_in-comm_out), self.hour)
        self.send_telegram_message(msg_out, type=type)
    
    def trade_image(self, data, action, price_i, price_o, time_i, time_o):
        '''Plot and save image of a trade

        Parameters:
            action (string): BUY or SELL
            price_i (float): entry price
            price_o (float): exit price
            time_i (string): entry time
            time_o (string): exit_time
        '''
        #register_get_auth_hook(self.arctic_auth_hook)
        #store = Arctic('157.245.223.103')
        #library = store['Futures_Historical_Ticks']
        time_in = str(pd.to_datetime(time_i) - timedelta(hours=2))
        time_out = str(pd.to_datetime(time_o) + timedelta(hours=2))
        data.index = pd.to_datetime(data.index)
        trade_data = data.loc[time_in:time_out]#library.read(self.symbol, date_range=DateRange(time_in, time_out)).data
        trade_data = self.resampler(data=trade_data, tempo='15Min', type='bars')#, type='ticks')
        
        c_in = (lambda act: 'g' if act == 'BUY' else 'r')(action)
        c_out = (lambda act: 'r' if act == 'BUY' else 'g')(action)
        dir_in = (lambda act: '^' if act == 'BUY' else 'v')(action)
        dir_out = (lambda act: 'v' if act == 'BUY' else '^')(action)
        bar_in = trade_data.index.get_loc(str(pd.to_datetime(time_i)), method='pad')
        bar_out = trade_data.index.get_loc(str(pd.to_datetime(time_o)), method='pad')
        
        util.barplot(trade_data, title='%s at %s-%s in %s'%(action, time_i, time_o, self.symbol), upColor='g')
        plt.scatter(bar_in, price_i, c=c_in, s=150, marker=dir_in, label='entry price: %.2f'%price_i)
        plt.scatter(bar_out, price_o, c=c_out, s=150, marker=dir_out, label='exit price: %.2f'%price_o)
        plt.legend()
        plt.savefig('%s_trades_images/%s at %s(%.2f) %s(%.2f) in %s.png'%(self.symbol, action, time_i.replace(':','.'), price_i, time_o.replace(':','.'), price_o, self.symbol))

    def entry_image(self, data, action, price_i, time_i, stop, target):
        '''Plot and save image of an entry

        Parameters:
            action (string): BUY or SELL
            price_i (float): entry price
            time_i (string): entry time
            stop (float): stop price
            target (float): target price
        '''
        #register_get_auth_hook(self.arctic_auth_hook)
        #store = Arctic('157.245.223.103')
        #library = store['Futures_Historical_Ticks']
        time_in = str(pd.to_datetime(time_i) - timedelta(hours=2))
        time_now = str(pd.to_datetime(time_i))
        data.index = pd.to_datetime(data.index)
        trade_data = data.loc[time_in:time_now]#library.read(self.symbol, date_range=DateRange(time_in, time_out)).data
        trade_data = self.resampler(data=trade_data, tempo='15Min', type='bars')#, type='ticks')

        c_in = (lambda act: 'g' if act == 'BUY' else 'r')(action)
        dir_in = (lambda act: '^' if act == 'BUY' else 'v')(action)
        bar_in = trade_data.index.get_loc(str(pd.to_datetime(time_i)), method='pad')

        util.barplot(trade_data, title='%s at %s in %s'%(action, time_i, self.symbol), upColor='g')
        plt.scatter(bar_in, price_i, c=c_in, s=150, marker=dir_in, label='entry price: %.2f'%price_i)
        plt.axhline(y=stop, color = 'red', linestyle='--', label='stop price: %.2f'%stop)
        plt.axhline(y=target, color = 'green', linestyle='--', label='target price: %.2f'%target)
        plt.axhline(y=price_i, color = 'grey', linestyle='--')
        plt.legend()
        plt.savefig('%s_entry_images/%s at %s(%.2f sl %.2f tp %.2f) in %s.png'%(self.symbol, action, time_i.replace(':','.'), price_i, stop, target, self.symbol))

    def save_position(self):
        '''Save current position in a CSV file
        '''
        pd.DataFrame({'position': self.position}, index=[0]).to_csv('../position.csv', index=False)
    
    def check_global_position(self):
        '''Read current global position
        '''
        return pd.read_csv('../position.csv').iloc[0].values[0]

    def check_margins(self, bot_name, qty, action, ord_type='market', price=0, max_stop=0):
        '''Check if there is enough margin according to init margin and max stop

        Parameters:
            qty (int): number of contracts
            action (string): order direction ('BUY' or 'SELL')
            ord_type (string): order type (market or limit)
            price (float): order price if ord_type is limit
            max_stop (float): maximum stop

        Returns:
            Boolean: True or False if Margin is enough
        '''
        allow_margin = False
        order = MarketOrder(action, int(qty)) if ord_type=='market' else StopOrder(action, int(qty), price) if ord_type=='stop' else LimitOrder(action, int(qty), price)
        order_data = util.tree(self.ib.whatIfOrder(self.contract, order))['OrderState']
        # Get margin values
        net_liq = float(order_data['equityWithLoanBefore'])
        margin_req = float(order_data['initMarginAfter'])
        if net_liq >= margin_req + max_stop:
            allow_margin = True
            self.print('Margin Accepted! %.2f USD more than required'%(net_liq - margin_req))
        else:
            allow_margin = False
            missing_founds = margin_req + max_stop - net_liq
            self.print('Margin is not enought! There are %.2f USD missing'%missing_founds)
            self.send_telegram_message('There are %.2f missing...'%abs(missing_founds))
        return allow_margin
