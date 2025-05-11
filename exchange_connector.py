# Create logs and analysis directories if they don't exist
#!/usr/bin/env python3
"""
Exchange Connector - Fetches real market data from public APIs and 
forwards it to the multicast publisher.

This module supports multiple data sources and implements fault tolerance.
"""
import asyncio
import os
import json
import time
import random
import datetime
import threading
import queue
import traceback
import ssl
import websockets
import requests
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
import logging

os.makedirs('logs', exist_ok=True)
os.makedirs('analysis', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/exchange_connector.log'
)
logger = logging.getLogger('exchange_connector')

# Configure rich console
console = Console()

class ExchangeConnector:
    """Base class for exchange connectors"""
    
    def __init__(self, exchange_name, symbols, output_queue):
        self.exchange_name = exchange_name
        self.symbols = symbols
        self.output_queue = output_queue
        self.running = False
        self.last_data_time = None
        self.connection_status = "DISCONNECTED"
        self.error_count = 0
        self.message_count = 0
    
    def start(self):
        """Start the connector in a separate thread"""
        self.running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()
        return self.thread
        
    def stop(self):
        """Stop the connector"""
        self.running = False
        self.connection_status = "STOPPED"
    
    def _run(self):
        """Main connector loop - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement _run()")
    
    def format_message(self, symbol, price, volume, bid=None, ask=None, timestamp=None):
        """Format market data into a standard message format"""
        if timestamp is None:
            timestamp = datetime.datetime.now()
            
        if isinstance(timestamp, (int, float)):
            # Convert unix timestamp to datetime
            timestamp = datetime.datetime.fromtimestamp(timestamp)
            
        ts_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
        
        # Create a standardized message format
        message = {
            "exchange": self.exchange_name,
            "symbol": symbol,
            "timestamp": ts_str,
            "price": float(price),
            "volume": float(volume) if volume is not None else None,
        }
        
        # Add optional fields if provided
        if bid is not None:
            message["bid"] = float(bid)
        if ask is not None:
            message["ask"] = float(ask)
            
        self.message_count += 1
        self.last_data_time = time.time()
        
        return message
    
    def get_status(self):
        """Return the current status of the connector"""
        return {
            "exchange": self.exchange_name,
            "status": self.connection_status,
            "messages": self.message_count,
            "errors": self.error_count,
            "last_data": self.last_data_time,
            "uptime": time.time() - self.last_data_time if self.last_data_time else None
        }


class CoinbaseConnector(ExchangeConnector):
    """Connector for Coinbase cryptocurrency exchange"""
    
    def __init__(self, symbols, output_queue):
        super().__init__("COINBASE", symbols, output_queue)
        # Convert symbols to Coinbase format (BTC -> BTC-USD)
        self.formatted_symbols = [f"{s}-USD" for s in symbols]
        self.websocket = None
    
    async def _connect_websocket(self):
        """Connect to Coinbase websocket API"""
        uri = "wss://ws-feed.exchange.coinbase.com"
        
        # Create subscription message
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": self.formatted_symbols,
            "channels": ["ticker"]
        }
        
        self.connection_status = "CONNECTING"
        console.print(f"[cyan]Connecting to Coinbase websocket...[/cyan]")
        
        try:
            self.websocket = await websockets.connect(uri)
            await self.websocket.send(json.dumps(subscribe_msg))
            self.connection_status = "CONNECTED"
            console.print(f"[green]Connected to Coinbase websocket[/green]")
            
            while self.running:
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=30)
                    await self._process_message(message)
                except asyncio.TimeoutError:
                    console.print("[yellow]Coinbase websocket timeout, sending heartbeat...[/yellow]")
                    try:
                        pong = await self.websocket.ping()
                        await asyncio.wait_for(pong, timeout=10)
                        console.print("[green]Heartbeat successful[/green]")
                    except:
                        console.print("[red]Heartbeat failed, reconnecting...[/red]")
                        self.connection_status = "RECONNECTING"
                        break
                except Exception as e:
                    self.error_count += 1
                    logger.error(f"Error processing Coinbase message: {str(e)}")
                    console.print(f"[red]Error processing message: {str(e)}[/red]")
                    if not self.running:
                        break
                    await asyncio.sleep(1)
            
        except Exception as e:
            self.error_count += 1
            self.connection_status = "ERROR"
            logger.error(f"Coinbase websocket error: {str(e)}")
            console.print(f"[red]Websocket connection error: {str(e)}[/red]")
            await asyncio.sleep(5)  # Wait before reconnecting
    
    async def _process_message(self, message_str):
        """Process incoming websocket messages"""
        try:
            message = json.loads(message_str)
            
            # Skip non-ticker messages
            if message.get('type') != 'ticker':
                return
                
            product_id = message.get('product_id', '')
            if '-USD' in product_id:
                symbol = product_id.replace('-USD', '')
            else:
                symbol = product_id
                
            price = message.get('price')
            if price is None:
                return
                
            volume = message.get('last_size')
            bid = message.get('best_bid')
            ask = message.get('best_ask')
            server_time = message.get('time')
            
            # Parse ISO timestamp if available
            timestamp = datetime.datetime.now()
            if server_time:
                try:
                    timestamp = datetime.datetime.fromisoformat(server_time.replace('Z', '+00:00'))
                    # Convert to local time
                    timestamp = timestamp.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
                except:
                    pass
            
            # Format and queue message
            formatted_msg = self.format_message(
                symbol=symbol,
                price=price,
                volume=volume,
                bid=bid,
                ask=ask,
                timestamp=timestamp
            )
            
            if random.random() > 0.95:  # Print occasional messages (not all)
                console.print(f"[dim][Coinbase] {symbol}: ${price} ({volume})[/dim]")
                
            self.output_queue.put(formatted_msg)
            
        except json.JSONDecodeError:
            self.error_count += 1
            logger.error(f"Failed to parse Coinbase message: {message_str}")
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing Coinbase message: {str(e)}")
            console.print(f"[red]Error processing Coinbase message: {str(e)}[/red]")
    
    def _run(self):
        """Run the Coinbase connector in an asyncio event loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while self.running:
            try:
                loop.run_until_complete(self._connect_websocket())
            except Exception as e:
                self.error_count += 1
                self.connection_status = "ERROR"
                logger.error(f"Coinbase connector error: {str(e)}")
                console.print(f"[red]Coinbase connector error: {str(e)}[/red]")
            
            if self.running:
                console.print("[yellow]Reconnecting to Coinbase in 5 seconds...[/yellow]")
                time.sleep(5)
                
        if self.websocket and not self.websocket.closed:
            loop.run_until_complete(self.websocket.close())
            
        loop.close()
        console.print("[yellow]Coinbase connector stopped[/yellow]")


class AlphaVantageConnector(ExchangeConnector):
    """Connector for Alpha Vantage API (stock market data)"""
    
    def __init__(self, symbols, output_queue, api_key):
        super().__init__("ALPHA_VANTAGE", symbols, output_queue)
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        
    def _run(self):
        """Poll Alpha Vantage API for stock data"""
        self.connection_status = "CONNECTED"
        
        while self.running:
            for symbol in self.symbols:
                if not self.running:
                    break
                    
                try:
                    # Get quote data
                    params = {
                        "function": "GLOBAL_QUOTE",
                        "symbol": symbol,
                        "apikey": self.api_key
                    }
                    
                    self.connection_status = "FETCHING"
                    response = requests.get(self.base_url, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if "Global Quote" in data and data["Global Quote"]:
                            quote = data["Global Quote"]
                            
                            price = quote.get("05. price")
                            volume = quote.get("06. volume")
                            
                            if price:
                                # Format and queue message
                                formatted_msg = self.format_message(
                                    symbol=symbol,
                                    price=price,
                                    volume=volume
                                )
                                
                                console.print(f"[dim][AlphaVantage] {symbol}: ${price} ({volume})[/dim]")
                                self.output_queue.put(formatted_msg)
                                self.connection_status = "CONNECTED"
                            else:
                                console.print(f"[yellow]No price data for {symbol}[/yellow]")
                                self.connection_status = "PARTIAL_DATA"
                        else:
                            console.print(f"[yellow]Invalid response for {symbol}: {data}[/yellow]")
                            self.error_count += 1
                            self.connection_status = "ERROR"
                    else:
                        console.print(f"[red]API error for {symbol}: {response.status_code}[/red]")
                        self.error_count += 1
                        self.connection_status = "ERROR"
                        
                except Exception as e:
                    self.error_count += 1
                    self.connection_status = "ERROR"
                    logger.error(f"Alpha Vantage error for {symbol}: {str(e)}")
                    console.print(f"[red]Alpha Vantage error for {symbol}: {str(e)}[/red]")
                
                # Rate limit to avoid API restrictions (5 calls per minute on free tier)
                time.sleep(15)
            
            # After processing all symbols, wait before next cycle
            time.sleep(5)


class YahooFinanceConnector(ExchangeConnector):
    """Connector for Yahoo Finance (uses unofficial API)"""
    
    def __init__(self, symbols, output_queue):
        super().__init__("YAHOO", symbols, output_queue)
        
    def _run(self):
        """Poll Yahoo Finance for stock data"""
        self.connection_status = "CONNECTED"
        
        while self.running:
            for symbol in self.symbols:
                if not self.running:
                    break
                    
                try:
                    # Use Yahoo Finance API
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                    params = {
                        "interval": "1m",
                        "range": "1d"
                    }
                    
                    self.connection_status = "FETCHING"
                    response = requests.get(url, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Extract the latest price
                        result = data.get("chart", {}).get("result", [{}])[0]
                        meta = result.get("meta", {})
                        
                        # Get the latest timestamp and corresponding price
                        timestamps = result.get("timestamp", [])
                        quotes = result.get("indicators", {}).get("quote", [{}])[0]
                        
                        if timestamps and quotes:
                            # Get the latest available data
                            price = None
                            for i in range(len(timestamps) - 1, -1, -1):
                                if "close" in quotes and i < len(quotes["close"]) and quotes["close"][i] is not None:
                                    price = quotes["close"][i]
                                    timestamp = timestamps[i]
                                    break
                            
                            if price is not None:
                                # Get volume if available
                                volume = None
                                if "volume" in quotes and i < len(quotes["volume"]) and quotes["volume"][i] is not None:
                                    volume = quotes["volume"][i]
                                
                                # Format and queue message
                                formatted_msg = self.format_message(
                                    symbol=symbol,
                                    price=price,
                                    volume=volume,
                                    timestamp=timestamp
                                )
                                
                                console.print(f"[dim][Yahoo] {symbol}: ${price} ({volume})[/dim]")
                                self.output_queue.put(formatted_msg)
                                self.connection_status = "CONNECTED"
                            else:
                                console.print(f"[yellow]No price data for {symbol}[/yellow]")
                                self.connection_status = "PARTIAL_DATA"
                        else:
                            console.print(f"[yellow]No time series data for {symbol}[/yellow]")
                            self.connection_status = "PARTIAL_DATA"
                    else:
                        console.print(f"[red]API error for {symbol}: {response.status_code}[/red]")
                        self.error_count += 1
                        self.connection_status = "ERROR"
                        
                except Exception as e:
                    self.error_count += 1
                    self.connection_status = "ERROR"
                    logger.error(f"Yahoo Finance error for {symbol}: {str(e)}")
                    console.print(f"[red]Yahoo Finance error for {symbol}: {str(e)}[/red]")
                
                # Rate limit to avoid API restrictions
                time.sleep(2)
            
            # After processing all symbols, wait before next cycle
            time.sleep(5)


class SimulatedExchangeConnector(ExchangeConnector):
    """Fallback simulator for when real exchange connections are not available"""
    
    def __init__(self, symbols, output_queue, mode="realistic"):
        super().__init__("SIMULATED", symbols, output_queue)
        self.mode = mode
        
        # Initialize price models for each symbol
        self.prices = {}
        for symbol in symbols:
            if symbol.startswith("BTC") or symbol.startswith("ETH"):
                # Crypto prices
                self.prices[symbol] = {
                    "price": random.uniform(25000, 70000) if symbol.startswith("BTC") else random.uniform(1500, 3500),
                    "volatility": random.uniform(0.001, 0.005)
                }
            else:
                # Stock prices
                self.prices[symbol] = {
                    "price": random.uniform(50, 500),
                    "volatility": random.uniform(0.0005, 0.002)
                }
    
    def _run(self):
        """Generate simulated market data"""
        self.connection_status = "CONNECTED"
        
        while self.running:
            for symbol in self.symbols:
                # Skip some updates randomly to simulate variable update rates
                if random.random() < 0.3:
                    continue
                    
                try:
                    # Get current price model
                    model = self.prices[symbol]
                    
                    # Update price using random walk with drift
                    price_change = random.normalvariate(0, 1) * model["volatility"] * model["price"]
                    # Add small upward drift (0.01%)
                    drift = model["price"] * 0.0001
                    new_price = model["price"] + price_change + drift
                    
                    # Ensure price doesn't go negative or too low
                    new_price = max(new_price, model["price"] * 0.2)
                    
                    model["price"] = new_price
                    
                    # Simulate volume
                    volume = random.randint(1, 1000) * 100
                    
                    # Add occasional price spikes or drops (market events)
                    if random.random() < 0.01:  # 1% chance of significant move
                        direction = 1 if random.random() < 0.5 else -1
                        magnitude = random.uniform(0.01, 0.05)  # 1-5% move
                        model["price"] *= (1 + direction * magnitude)
                        volume *= random.randint(2, 10)  # Higher volume during big moves
                    
                    # Format and queue message
                    formatted_msg = self.format_message(
                        symbol=symbol,
                        price=round(model["price"], 2),
                        volume=volume
                    )
                    
                    if random.random() > 0.95:  # Only print occasional updates
                        console.print(f"[dim][Simulated] {symbol}: ${round(model['price'], 2)} ({volume})[/dim]")
                        
                    self.output_queue.put(formatted_msg)
                    
                except Exception as e:
                    self.error_count += 1
                    logger.error(f"Simulation error for {symbol}: {str(e)}")
                
                # Small random delay between symbols
                time.sleep(random.uniform(0.05, 0.2))
            
            # Random delay between cycles
            time.sleep(random.uniform(0.5, 1.5))


class ConnectorManager:
    """Manages multiple exchange connectors and consolidates their data"""
    
    def __init__(self):
        self.connectors = {}
        self.data_queue = queue.Queue()
        self.running = False
        console.print(Panel.fit("[bold green]Exchange Connector Manager[/bold green]"))
    
    def add_connector(self, connector):
        """Add a connector to the manager"""
        self.connectors[connector.exchange_name] = connector
        console.print(f"[green]Added connector for {connector.exchange_name}[/green]")
        return connector
    
    def start_all(self):
        """Start all connectors"""
        self.running = True
        for name, connector in self.connectors.items():
            connector.start()
            console.print(f"[green]Started connector for {name}[/green]")
    
    def stop_all(self):
        """Stop all connectors"""
        self.running = False
        for name, connector in self.connectors.items():
            connector.stop()
            console.print(f"[yellow]Stopped connector for {name}[/yellow]")
    
    def get_data_queue(self):
        """Get the consolidated data queue"""
        return self.data_queue
    
    def get_status(self):
        """Get status of all connectors"""
        status = {}
        for name, connector in self.connectors.items():
            status[name] = connector.get_status()
        return status
    
    def print_status(self):
        """Print status of all connectors"""
        status_table = Table(title="Exchange Connector Status")
        status_table.add_column("Exchange", style="cyan")
        status_table.add_column("Status", style="green")
        status_table.add_column("Messages", style="blue")
        status_table.add_column("Errors", style="red")
        status_table.add_column("Last Data", style="magenta")
        
        for name, connector in self.connectors.items():
            status = connector.get_status()
            
            last_data = "Never"
            if status["last_data"]:
                seconds_ago = time.time() - status["last_data"]
                if seconds_ago < 60:
                    last_data = f"{int(seconds_ago)}s ago"
                else:
                    last_data = f"{int(seconds_ago / 60)}m ago"
            
            status_table.add_row(
                name,
                status["status"],
                str(status["messages"]),
                str(status["errors"]),
                last_data
            )
            
        console.print(status_table)


# Main execution when run as script
if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    import os
    os.makedirs('logs', exist_ok=True)
    
    # Sample usage
    manager = ConnectorManager()
    
    # Add connectors with different market symbols
    # 1. Crypto connector (Coinbase)
    crypto_connector = CoinbaseConnector(
        symbols=["BTC", "ETH", "SOL", "DOGE"],
        output_queue=manager.get_data_queue()
    )
    manager.add_connector(crypto_connector)
    
    # 2. Stock connector using Yahoo Finance
    stock_connector = YahooFinanceConnector(
        symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
        output_queue=manager.get_data_queue()
    )
    manager.add_connector(stock_connector)
    
    # 3. Add simulated connector as a fallback
    sim_connector = SimulatedExchangeConnector(
        symbols=["IBM", "NFLX", "FB", "NVDA", "AMD", "LTC", "XRP"],
        output_queue=manager.get_data_queue()
    )
    manager.add_connector(sim_connector)
    
    try:
        # Start all connectors
        manager.start_all()
        
        console.print("[bold green]All connectors started. Press Ctrl+C to stop.[/bold green]")
        
        # Main loop - process messages from the queue
        while True:
            try:
                # Print status every 30 seconds
                manager.print_status()
                time.sleep(30)
            except Exception as e:
                console.print(f"[red]Error in main loop: {str(e)}[/red]")
                time.sleep(5)
            
    except KeyboardInterrupt:
        console.print("[yellow]Shutting down connectors...[/yellow]")
        manager.stop_all()
        console.print("[bold]Connectors stopped[/bold]")
