# Create logs and analysis directories if they don't exist
#!/usr/bin/env python3
"""
Real Market Data Multicast Receiver

Receives market data from multicast publisher, performs:
- Sequence validation
- Latency measurement
- Data quality analysis
- Trade simulation
"""
import socket
import struct
import select
import time
import datetime
import json
import argparse
import threading
import os
import sys
import logging
import statistics
from collections import defaultdict, deque
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich import box

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/multicast_receiver.log'
)
logger = logging.getLogger('multicast_receiver')

# Configure rich console
console = Console()

class MarketDataReceiver:
    """
    Receives and processes multicast market data
    """
    
    def __init__(self, 
                 mcast_grp='224.1.1.1', 
                 port=5007,
                 log_file=None,
                 verbose=False,
                 display_mode="full",
                 stats_only=False):
        
        self.mcast_grp = mcast_grp
        self.port = port
        self.log_file = log_file
        self.verbose = verbose
        self.display_mode = display_mode
        self.stats_only = stats_only
        
        # Prepare socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', port))
        
        # Join multicast group
        mreq = struct.pack("4sl", socket.inet_aton(mcast_grp), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        # Initialize state
        self.running = False
        self.start_time = time.time()
        
        # Message counters
        self.messages_received = 0
        self.messages_dropped = 0
        self.last_id = 0
        self.byte_count = 0
        
        # Data structures for market data
        self.latest_prices = {}  # symbol -> price
        self.price_history = defaultdict(lambda: deque(maxlen=100))  # symbol -> list of prices
        self.volume_history = defaultdict(lambda: deque(maxlen=100))  # symbol -> list of volumes
        
        # Latency tracking
        self.latencies = deque(maxlen=1000)  # Store recent latencies
        
        # Exchange tracking
        self.exchange_counts = defaultdict(int)  # exchange -> count
        
        # Log file setup
        self.log_file_handle = None
        if log_file:
            try:
                self.log_file_handle = open(log_file, 'w')
                # Write CSV header
                header = "timestamp,exchange,symbol,price,volume,msg_id,latency_ms\n"
                self.log_file_handle.write(header)
                self.log_file_handle.flush()
            except Exception as e:
                console.print(f"[red]Error opening log file: {str(e)}[/red]")
                logger.error(f"Error opening log file: {str(e)}")
                self.log_file_handle = None
        
        # Trade simulation
        self.portfolio = defaultdict(float)  # symbol -> quantity
        self.cash = 1000000.0  # Initial cash
        self.portfolio_value_history = deque(maxlen=100)  # Track portfolio value over time
        self.trade_history = deque(maxlen=50)  # Recent trades
        
        # Initialize display layout
        self._init_display_layout()
        
        console.print(Panel.fit(
            f"[bold blue]Real-time Market Data Receiver[/bold blue]\n"
            f"Listening on [bold cyan]{mcast_grp}:{port}[/bold cyan]\n" +
            (f"Logging to [bold green]{log_file}[/bold green]" if log_file else "")
        ))
    
    def _init_display_layout(self):
        """Initialize the display layout"""
        self.layout = Layout()
        
        # Create main sections
        self.layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )
        
        # Split the body section based on display mode
        if self.display_mode == "full":
            self.layout["body"].split_row(
                Layout(name="prices", ratio=2),
                Layout(name="stats", ratio=1)
            )
            
            # Split stats section
            self.layout["stats"].split_column(
                Layout(name="message_stats"),
                Layout(name="latency_stats"),
                Layout(name="portfolio_stats")
            )
        elif self.display_mode == "prices":
            self.layout["body"].split_row(
                Layout(name="prices", ratio=1)
            )
        elif self.display_mode == "stats":
            self.layout["body"].split_row(
                Layout(name="stats", ratio=1)
            )
            
            # Split stats section
            self.layout["stats"].split_column(
                Layout(name="message_stats"),
                Layout(name="latency_stats")
            )
        elif self.display_mode == "portfolio":
            self.layout["body"].split_row(
                Layout(name="portfolio", ratio=1)
            )
    
    def _generate_header(self):
        """Generate the header display"""
        text = Text()
        text.append("Real-time Market Data Receiver\n")
        text.append(f"Multicast: {self.mcast_grp}:{self.port} | ")
        text.append(f"Uptime: {time.time() - self.start_time:.1f}s | ")
        text.append(f"Msgs: {self.messages_received} | ")
        text.append(f"Drops: {self.messages_dropped}\n")
        return Panel(text, box=box.ROUNDED)
    
    def _generate_footer(self):
        """Generate the footer display"""
        text = Text()
        text.append("Press Ctrl+C to exit | ")
        if self.log_file:
            text.append(f"Logging to {self.log_file} | ")
        text.append(f"Updated: {datetime.datetime.now().strftime('%H:%M:%S')}")
        return Panel(text, box=box.ROUNDED)
    
    def _generate_price_table(self):
        """Generate the price table display"""
        price_table = Table(title="Latest Market Data", box=box.SIMPLE)
        price_table.add_column("Symbol", style="cyan")
        price_table.add_column("Exchange", style="green")
        price_table.add_column("Price", style="yellow", justify="right")
        price_table.add_column("Volume", justify="right")
        price_table.add_column("Change", justify="right")
        
        # Sort symbols by price (descending)
        sorted_symbols = sorted(
            self.latest_prices.keys(), 
            key=lambda s: self.latest_prices.get(s, {}).get('price', 0),
            reverse=True
        )
        
        for symbol in sorted_symbols:
            data = self.latest_prices.get(symbol, {})
            price = data.get('price', 0)
            exchange = data.get('exchange', 'Unknown')
            volume = data.get('volume', 0)
            
            # Calculate price change if history exists
            change = ""
            history = self.price_history.get(symbol, [])
            if len(history) > 1:
                previous = history[-2] if len(history) >= 2 else history[0]
                pct_change = ((price - previous) / previous) * 100 if previous > 0 else 0
                
                # Format with color based on direction
                if pct_change > 0:
                    change = f"[green]+{pct_change:.2f}%[/green]"
                elif pct_change < 0:
                    change = f"[red]{pct_change:.2f}%[/red]"
                else:
                    change = "0.00%"
            
            # Format price with 2 decimal places for stocks, more for crypto
            if symbol in ["BTC", "ETH", "SOL", "DOGE", "ADA", "XRP"]:
                price_str = f"${price:.2f}"
            else:
                price_str = f"${price:.2f}"
                
            # Format volume
            if volume:
                volume_str = f"{volume:,.0f}" if volume >= 1 else f"{volume:.4f}"
            else:
                volume_str = ""
                
            price_table.add_row(symbol, exchange, price_str, volume_str, change)
            
        return price_table
    
    def _generate_message_stats(self):
        """Generate message statistics table"""
        elapsed = time.time() - self.start_time
        
        stats_table = Table(title="Message Statistics", box=box.SIMPLE)
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="green")
        
        # Calculate rates
        msg_rate = self.messages_received / elapsed if elapsed > 0 else 0
        bandwidth = (self.byte_count / 1024) / elapsed if elapsed > 0 else 0
        drop_rate = self.messages_dropped / max(self.messages_received + self.messages_dropped, 1)
        
        # Add rows to table
        stats_table.add_row("Messages Received", f"{self.messages_received:,}")
        stats_table.add_row("Messages Dropped", f"{self.messages_dropped:,}")
        stats_table.add_row("Drop Rate", f"{drop_rate:.2%}")
        stats_table.add_row("Message Rate", f"{msg_rate:.2f} msgs/sec")
        stats_table.add_row("Data Received", f"{self.byte_count / 1024:.2f} KB")
        stats_table.add_row("Bandwidth", f"{bandwidth:.2f} KB/sec")
        stats_table.add_row("Symbols Tracked", f"{len(self.latest_prices)}")
        
        # Add exchange breakdown
        if self.exchange_counts:
            stats_table.add_row("", "")
            stats_table.add_row("[bold]Exchange Breakdown[/bold]", "")
            
            for exchange, count in sorted(self.exchange_counts.items(), key=lambda x: x[1], reverse=True):
                stats_table.add_row(exchange, f"{count:,} msgs")
                
        return stats_table
    
    def _generate_latency_stats(self):
        """Generate latency statistics table"""
        latency_table = Table(title="Latency Statistics", box=box.SIMPLE)
        latency_table.add_column("Metric", style="cyan")
        latency_table.add_column("Value", style="green")
        
        if self.latencies:
            min_latency = min(self.latencies)
            max_latency = max(self.latencies)
            avg_latency = sum(self.latencies) / len(self.latencies)
            
            # Use percentiles for more stable results
            p50 = statistics.median(self.latencies)
            
            # Calculate 95th and 99th percentiles if we have enough data
            p95 = p99 = None
            if len(self.latencies) >= 20:
                sorted_latencies = sorted(self.latencies)
                idx_95 = int(len(sorted_latencies) * 0.95)
                idx_99 = int(len(sorted_latencies) * 0.99)
                p95 = sorted_latencies[idx_95]
                p99 = sorted_latencies[idx_99]
            
            latency_table.add_row("Minimum Latency", f"{min_latency:.2f} ms")
            latency_table.add_row("Average Latency", f"{avg_latency:.2f} ms")
            latency_table.add_row("Median Latency (P50)", f"{p50:.2f} ms")
            if p95 is not None:
                latency_table.add_row("95th Percentile (P95)", f"{p95:.2f} ms")
            if p99 is not None:
                latency_table.add_row("99th Percentile (P99)", f"{p99:.2f} ms")
            latency_table.add_row("Maximum Latency", f"{max_latency:.2f} ms")
            latency_table.add_row("Samples", f"{len(self.latencies):,}")
        else:
            latency_table.add_row("No latency data", "")
            
        return latency_table
    
    def _generate_portfolio_stats(self):
        """Generate portfolio statistics and trade history"""
        portfolio_table = Table(title="Portfolio Statistics", box=box.SIMPLE)
        portfolio_table.add_column("Asset", style="cyan")
        portfolio_table.add_column("Quantity", style="green", justify="right")
        portfolio_table.add_column("Price", style="yellow", justify="right")
        portfolio_table.add_column("Value", style="magenta", justify="right")
        
        # Calculate total portfolio value
        total_value = self.cash
        for symbol, qty in self.portfolio.items():
            price_data = self.latest_prices.get(symbol, {})
            price = price_data.get('price', 0)
            value = qty * price
            total_value += value
            
            if qty > 0:
                portfolio_table.add_row(
                    symbol,
                    f"{qty:,.4f}" if qty < 1 else f"{qty:,.2f}",
                    f"${price:,.2f}",
                    f"${value:,.2f}"
                )
        
        # Add cash and total
        portfolio_table.add_row("Cash", "", "", f"${self.cash:,.2f}")
        portfolio_table.add_row("[bold]Total Value[/bold]", "", "", f"[bold]${total_value:,.2f}[/bold]")
        
        # Record portfolio value history
        self.portfolio_value_history.append(total_value)
        
        # Add recent trades if we have any
        if self.trade_history:
            portfolio_table.add_row("", "", "", "")
            portfolio_table.add_row("[bold]Recent Trades[/bold]", "", "", "")
            
            for trade in list(self.trade_history)[-5:]:  # Show last 5 trades
                action = trade.get('action')
                symbol = trade.get('symbol')
                qty = trade.get('quantity')
                price = trade.get('price')
                value = trade.get('value')
                
                action_str = "[green]BUY[/green]" if action == "BUY" else "[red]SELL[/red]"
                portfolio_table.add_row(
                    f"{action_str} {symbol}",
                    f"{qty:,.4f}" if qty < 1 else f"{qty:,.2f}",
                    f"${price:,.2f}",
                    f"${value:,.2f}"
                )
                
        return portfolio_table
    
    def update_display(self, live):
        """Update the live display"""
        # Update layout sections
        self.layout["header"].update(self._generate_header())
        self.layout["footer"].update(self._generate_footer())
        
        # Update body sections based on display mode
        if self.display_mode == "full" or self.display_mode == "prices":
            self.layout["prices"].update(self._generate_price_table())
            
        if self.display_mode == "full" or self.display_mode == "stats":
            self.layout["message_stats"].update(self._generate_message_stats())
            self.layout["latency_stats"].update(self._generate_latency_stats())
            
        if self.display_mode == "full":
            self.layout["portfolio_stats"].update(self._generate_portfolio_stats())
            
        if self.display_mode == "portfolio":
            self.layout["body"].update(self._generate_portfolio_stats())
    
    def log_message(self, message, latency_ms):
        """Log a message to the log file"""
        if not self.log_file_handle:
            return
            
        try:
            # Format: timestamp,exchange,symbol,price,volume,msg_id,latency_ms
            log_entry = (
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]},"
                f"{message.get('exchange', 'unknown')},"
                f"{message.get('symbol', 'unknown')},"
                f"{message.get('price', 0)},"
                f"{message.get('volume', '')},"
                f"{message.get('id', -1)},"
                f"{latency_ms:.2f}\n"
            )
            
            self.log_file_handle.write(log_entry)
            self.log_file_handle.flush()
            
        except Exception as e:
            logger.error(f"Error writing to log file: {str(e)}")
            if self.verbose:
                console.print(f"[red]Error writing to log file: {str(e)}[/red]")
    
    def calculate_latency(self, message):
        """Calculate message latency in milliseconds"""
        try:
            # If message has a send_time field, use it for latency calculation
            if 'send_time' in message:
                send_time_str = message['send_time']
                send_time = datetime.datetime.strptime(f"{datetime.datetime.now().strftime('%Y-%m-%d')} {send_time_str}", 
                                                    "%Y-%m-%d %H:%M:%S.%f")
                receive_time = datetime.datetime.now()
                
                # Calculate latency in milliseconds
                latency_ms = (receive_time - send_time).total_seconds() * 1000
                return latency_ms
        except Exception as e:
            logger.warning(f"Error calculating latency: {str(e)}")
            
        return None
    
    def execute_trading_strategy(self, message):
        """Execute a simple trading strategy based on price movements"""
        symbol = message.get('symbol')
        current_price = message.get('price')
        
        if not symbol or not current_price:
            return
            
        # Simple moving average crossover strategy
        price_history = self.price_history.get(symbol, [])
        
        # Need at least 20 data points for strategy
        if len(price_history) < 20:
            return
            
        # Calculate short and long moving averages
        short_period = 5
        long_period = 20
        
        short_ma = sum(price_history[-short_period:]) / short_period
        long_ma = sum(price_history[-long_period:]) / long_period
        
        # Get previous moving averages if possible
        if len(price_history) > long_period + 1:
            prev_short_ma = sum(price_history[-(short_period+1):-1]) / short_period
            prev_long_ma = sum(price_history[-(long_period+1):-1]) / long_period
            
            # Check for crossover
            signal = None
            
            # Buy signal: short MA crosses above long MA
            if prev_short_ma <= prev_long_ma and short_ma > long_ma:
                signal = "BUY"
                
            # Sell signal: short MA crosses below long MA
            elif prev_short_ma >= prev_long_ma and short_ma < long_ma:
                signal = "SELL"
                
            # Execute the trade if we have a signal
            if signal:
                self._execute_trade(symbol, current_price, signal)
    
    def _execute_trade(self, symbol, price, action):
        """Execute a simulated trade"""
        if action == "BUY":
            # Calculate position size (2% of portfolio value)
            portfolio_value = self.cash
            for sym, qty in self.portfolio.items():
                sym_price = self.latest_prices.get(sym, {}).get('price', 0)
                portfolio_value += qty * sym_price
                
            position_value = portfolio_value * 0.02  # 2% position size
            
            # Check if we have enough cash
            if position_value <= self.cash:
                quantity = position_value / price
                self.cash -= position_value
                self.portfolio[symbol] += quantity
                
                # Record the trade
                self.trade_history.append({
                    'action': action,
                    'symbol': symbol,
                    'quantity': quantity,
                    'price': price,
                    'value': position_value,
                    'timestamp': datetime.datetime.now()
                })
                
                if self.verbose:
                    console.print(f"[green]BUY {quantity:.4f} {symbol} @ ${price:.2f} = ${position_value:.2f}[/green]")
        
        elif action == "SELL":
            # Check if we have a position in this symbol
            if symbol in self.portfolio and self.portfolio[symbol] > 0:
                # Sell half the position
                quantity = self.portfolio[symbol] * 0.5
                value = quantity * price
                
                self.portfolio[symbol] -= quantity
                self.cash += value
                
                # Record the trade
                self.trade_history.append({
                    'action': action,
                    'symbol': symbol,
                    'quantity': quantity,
                    'price': price,
                    'value': value,
                    'timestamp': datetime.datetime.now()
                })
                
                if self.verbose:
                    console.print(f"[red]SELL {quantity:.4f} {symbol} @ ${price:.2f} = ${value:.2f}[/red]")
    
    def process_message(self, data, addr):
        """Process a received message"""
        try:
            # Decode and parse message
            message = json.loads(data.decode())
            
            # Update counters
            self.messages_received += 1
            self.byte_count += len(data)
            
            # Extract fields
            msg_id = message.get('id', -1)
            symbol = message.get('symbol')
            price = message.get('price')
            volume = message.get('volume')
            exchange = message.get('exchange', 'unknown')
            
            # Track exchanges
            self.exchange_counts[exchange] += 1
            
            # Validate sequence if we have an ID
            if msg_id > 0 and self.last_id > 0 and msg_id > self.last_id + 1:
                dropped = msg_id - self.last_id - 1
                self.messages_dropped += dropped
                
                if self.verbose:
                    console.print(f"[bold red]SEQUENCE GAP DETECTED! Missing {dropped} messages between ID:{self.last_id} and ID:{msg_id}[/bold red]")
                    
                logger.warning(f"Sequence gap: missing {dropped} messages between {self.last_id} and {msg_id}")
            
            self.last_id = max(self.last_id, msg_id)
            
            # Calculate latency
            latency_ms = self.calculate_latency(message)
            if latency_ms is not None:
                self.latencies.append(latency_ms)
            
            # Update market data
            if symbol and price:
                # Store latest price
                self.latest_prices[symbol] = {
                    'price': float(price),
                    'volume': volume,
                    'exchange': exchange,
                    'timestamp': datetime.datetime.now()
                }
                
                # Update history
                self.price_history[symbol].append(float(price))
                if volume:
                    self.volume_history[symbol].append(float(volume))
                
                # Run trading strategy
                self.execute_trading_strategy(message)
            
            # Log the message
            if self.log_file_handle and latency_ms is not None:
                self.log_message(message, latency_ms)
            
            # Print message if verbose
            if self.verbose and not self.stats_only:
                latency_str = f" (Latency: {latency_ms:.2f}ms)" if latency_ms is not None else ""
                console.print(f"[blue]Received from {addr}:[/blue] {json.dumps(message)}{latency_str}")
                
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON message: {data}")
            if self.verbose:
                console.print(f"[red]Error: Failed to decode JSON message[/red]")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            if self.verbose:
                console.print(f"[red]Error processing message: {str(e)}[/red]")
    
    def start(self):
        """Start the receiver"""
        self.running = True
        logger.info(f"Receiver started on {self.mcast_grp}:{self.port}")
        
        # Create a live display
        with Live(self.layout, refresh_per_second=2, screen=True) as live:
            try:
                # Set socket to non-blocking mode
                self.sock.setblocking(0)
                
                last_display_update = time.time()
                
                while self.running:
                    # Update the display periodically
                    current_time = time.time()
                    if current_time - last_display_update > 0.5:  # Update display every 0.5 seconds
                        self.update_display(live)
                        last_display_update = current_time
                    
                    # Try to receive data with timeout
                    try:
                        # Use select to wait for data with timeout
                        ready = select.select([self.sock], [], [], 0.1)
                        if ready[0]:
                            data, addr = self.sock.recvfrom(8192)
                            self.process_message(data, addr)
                    except socket.timeout:
                        pass
                    except BlockingIOError:
                        # No data available, just continue
                        time.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving data: {str(e)}")
                        if self.verbose:
                            console.print(f"[red]Error receiving data: {str(e)}[/red]")
                        time.sleep(0.1)
                        
            except KeyboardInterrupt:
                console.print("[yellow]Receiver stopping...[/yellow]")
            finally:
                self.stop()
    
    def stop(self):
        """Stop the receiver"""
        self.running = False
        self.sock.close()
        
        if self.log_file_handle:
            self.log_file_handle.close()
            
        # Print final statistics
        console.print("\n")
        console.print(self._generate_message_stats())
        console.print("\n")
        console.print(self._generate_latency_stats())
        console.print("\n")
        
        logger.info("Receiver stopped")
        console.print("[bold]Receiver stopped[/bold]")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Real-time Market Data Multicast Receiver')
    
    # Multicast parameters
    parser.add_argument('--group', type=str, default='224.1.1.1',
                        help='Multicast group address (default: 224.1.1.1)')
    parser.add_argument('--port', type=int, default=5007,
                        help='UDP port (default: 5007)')
    
    # Logging and display options
    parser.add_argument('--log-file', type=str,
                        help='Log file path (optional)')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--display', type=str, choices=['full', 'prices', 'stats', 'portfolio'], default='full',
                        help='Display mode (default: full)')
    parser.add_argument('--stats-only', action='store_true',
                        help='Show only statistics, not individual messages')
    
    args = parser.parse_args()
    return args


def main():
    """Main entry point"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Create log file path if specified
    log_file = None
    if args.log_file:
        log_file = args.log_file
    else:
        # Auto-generate log file name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/market_data_{timestamp}.csv"
    
    # Create and start receiver
    receiver = MarketDataReceiver(
        mcast_grp=args.group,
        port=args.port,
        log_file=log_file,
        verbose=args.verbose,
        display_mode=args.display,
        stats_only=args.stats_only
    )
    
    try:
        receiver.start()
    except KeyboardInterrupt:
        pass
    finally:
        receiver.stop()


if __name__ == "__main__":
    main()