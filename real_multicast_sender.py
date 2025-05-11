#!/usr/bin/env python3
"""
Exchange Connector - Fetches real market data from public APIs and 
forwards it to the multicast publisher.

This module supports multiple data sources and implements fault tolerance.
"""
import os
import ssl
import json
import argparse
import time
import queue
import socket
import random
import logging
import asyncio
import datetime
import threading
import traceback
import requests
import websockets

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, track






# Import the exchange connector
from exchange_connector import (
    ConnectorManager,
    CoinbaseConnector,
    YahooFinanceConnector,
    SimulatedExchangeConnector
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/multicast_publisher.log'
)
logger = logging.getLogger('multicast_publisher')

# Configure rich console
console = Console()

# Create logs and analysis directories if they don't exist
os.makedirs('logs', exist_ok=True)
os.makedirs('analysis', exist_ok=True)

class MarketDataPublisher:
    """
    Publishes market data to a multicast group.
    """
    
    def __init__(self, 
                 mcast_grp='224.1.1.1', 
                 port=5007, 
                 ttl=1,
                 drop_rate=0.001,
                 latency_min=0,
                 latency_max=1,
                 stats_interval=10):
        
        self.mcast_grp = mcast_grp
        self.port = port
        self.ttl = ttl
        self.drop_rate = drop_rate  # Packet drop simulation (0.0 - 1.0)
        self.latency_min = latency_min  # Minimum artificial latency (ms)
        self.latency_max = latency_max  # Maximum artificial latency (ms)
        self.stats_interval = stats_interval  # Print stats every N seconds
        
        # Create the UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        
        # Initialize counters
        self.msg_id = 1
        self.messages_sent = 0
        self.messages_dropped = 0
        self.packets_sent = 0
        self.bytes_sent = 0
        self.start_time = time.time()
        self.last_stats_time = time.time()
        
        # Create message buffer for artificial latency simulation
        self.message_buffer = []
        self.buffer_lock = threading.Lock()
        self.running = False

        # Log initialization
        logger.info(f"Initialized publisher: {mcast_grp}:{port}, TTL={ttl}, " + 
                   f"drop_rate={drop_rate}, latency={latency_min}-{latency_max}ms")
        
        console.print(Panel.fit(
            f"[bold green]Real-time Market Data Publisher[/bold green]\n"
            f"Broadcasting to [bold cyan]{mcast_grp}:{port}[/bold cyan] with TTL={ttl}\n"
            f"Packet drop simulation: {drop_rate*100:.1f}%\n"
            f"Artificial latency: {latency_min}-{latency_max}ms"
        ))
    
    def set_drop_rate(self, rate):
        """Set packet drop simulation rate"""
        if 0 <= rate <= 1:
            self.drop_rate = rate
            logger.info(f"Drop rate set to {rate}")
            console.print(f"[green]Drop rate set to {rate*100:.1f}%[/green]")
        else:
            console.print("[red]Drop rate must be between 0.0 and 1.0[/red]")
    
    def set_latency(self, min_ms, max_ms):
        """Set artificial latency range"""
        if 0 <= min_ms <= max_ms:
            self.latency_min = min_ms
            self.latency_max = max_ms
            logger.info(f"Latency range set to {min_ms}-{max_ms}ms")
            console.print(f"[green]Latency range set to {min_ms}-{max_ms}ms[/green]")
        else:
            console.print("[red]Invalid latency range[/red]")
    
    def publish_message(self, message):
        """
        Publish a message to the multicast group with optional artificial latency.
        """
        # Add message ID and original send timestamp
        message["id"] = self.msg_id
        self.msg_id += 1
        
        # Add original send timestamp (useful for latency measurement)
        if "send_time" not in message:
            message["send_time"] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        self.messages_sent += 1
        
        # Simulate packet drop
        if random.random() < self.drop_rate:
            self.messages_dropped += 1
            console.print(f"[red][DROP SIMULATED][/red] ID:{message['id']}")
            logger.info(f"Dropped message ID:{message['id']}")
            return
        
        # Calculate artificial latency for this message
        if self.latency_max > 0:
            latency = random.uniform(self.latency_min, self.latency_max) / 1000.0  # Convert to seconds
            
            # Add to delayed buffer with scheduled send time
            with self.buffer_lock:
                self.message_buffer.append({
                    "message": message,
                    "send_time": time.time() + latency
                })
        else:
            # Send immediately
            self._send_message(message)
    
    def _send_message(self, message):
        """Actually send the message to the multicast group"""
        try:
            # Convert to JSON and encode
            json_msg = json.dumps(message)
            encoded_msg = json_msg.encode()
            
            # Send to multicast group
            self.sock.sendto(encoded_msg, (self.mcast_grp, self.port))
            
            # Update statistics
            self.packets_sent += 1
            self.bytes_sent += len(encoded_msg)
            
            # Occasionally print sent message (not too verbose)
            if message.get('id', 0) % 20 == 0:
                console.print(f"[green]Sent:[/green] ID:{message.get('id')} Symbol:{message.get('symbol')} Price:{message.get('price')}")
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            console.print(f"[red]Error sending message: {str(e)}[/red]")
    
    def process_latency_buffer(self):
        """Process the latency buffer and send delayed messages"""
        while self.running:
            current_time = time.time()
            messages_to_send = []
            
            # Find messages ready to send
            with self.buffer_lock:
                remaining_messages = []
                for buffered_msg in self.message_buffer:
                    if current_time >= buffered_msg["send_time"]:
                        messages_to_send.append(buffered_msg["message"])
                    else:
                        remaining_messages.append(buffered_msg)
                
                # Update buffer with remaining messages
                self.message_buffer = remaining_messages
            
            # Send ready messages
            for message in messages_to_send:
                self._send_message(message)
            
            # Print statistics periodically
            if (current_time - self.last_stats_time) >= self.stats_interval:
                self.print_statistics()
                self.last_stats_time = current_time
            
            # Short sleep to prevent CPU spinning
            time.sleep(0.001)
    
    def print_statistics(self):
        """Print publisher statistics"""
        elapsed = time.time() - self.start_time
        
        stats_table = Table(title="Publisher Statistics")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="green")
        
        # Calculate rates
        msg_rate = self.messages_sent / elapsed if elapsed > 0 else 0
        packet_rate = self.packets_sent / elapsed if elapsed > 0 else 0
        bandwidth = (self.bytes_sent / 1024) / elapsed if elapsed > 0 else 0
        drop_rate = self.messages_dropped / self.messages_sent if self.messages_sent > 0 else 0
        
        # Add rows to table
        stats_table.add_row("Messages Processed", str(self.messages_sent))
        stats_table.add_row("Messages Dropped", str(self.messages_dropped))
        stats_table.add_row("Actual Drop Rate", f"{drop_rate:.2%}")
        stats_table.add_row("Packets Sent", str(self.packets_sent))
        stats_table.add_row("Bytes Sent", f"{self.bytes_sent / 1024:.2f} KB")
        stats_table.add_row("Message Rate", f"{msg_rate:.2f} msgs/sec")
        stats_table.add_row("Packet Rate", f"{packet_rate:.2f} packets/sec")
        stats_table.add_row("Bandwidth", f"{bandwidth:.2f} KB/sec")
        stats_table.add_row("Buffer Size", str(len(self.message_buffer)))
        stats_table.add_row("Uptime", f"{elapsed:.1f} seconds")
        
        console.print(stats_table)
        
        # Log statistics
        logger.info(f"Stats: msgs={self.messages_sent}, dropped={self.messages_dropped}, " +
                    f"rate={msg_rate:.2f} msgs/s, bandwidth={bandwidth:.2f} KB/s")
    
    def start(self, connector_manager):
        """Start the publisher"""
        self.running = True
        
        # Start latency buffer processor in a separate thread
        self.latency_thread = threading.Thread(target=self.process_latency_buffer)
        self.latency_thread.daemon = True
        self.latency_thread.start()
        
        # Get the data queue from the connector manager
        data_queue = connector_manager.get_data_queue()
        
        console.print("[bold green]Publisher started[/bold green]")
        logger.info("Publisher started")
        
        # Main loop - process messages from connectors
        try:
            while self.running:
                try:
                    # Get message from queue with timeout
                    message = data_queue.get(timeout=1.0)
                    self.publish_message(message)
                    data_queue.task_done()
                except queue.Empty:
                    # No messages in queue, just continue
                    pass
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    console.print(f"[red]Error processing message: {str(e)}[/red]")
        
        except KeyboardInterrupt:
            console.print("[yellow]Publisher stopping...[/yellow]")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the publisher"""
        self.running = False
        if hasattr(self, 'latency_thread') and self.latency_thread.is_alive():
            self.latency_thread.join(timeout=2.0)
        self.sock.close()
        console.print("[bold]Publisher stopped[/bold]")
        logger.info("Publisher stopped")
        # Print final statistics
        self.print_statistics()


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Real-time Market Data Multicast Publisher')
    
    # Multicast parameters
    parser.add_argument('--group', type=str, default='224.1.1.1',
                        help='Multicast group address (default: 224.1.1.1)')
    parser.add_argument('--port', type=int, default=5007,
                        help='UDP port (default: 5007)')
    parser.add_argument('--ttl', type=int, default=1,
                        help='Time-to-live for multicast packets (default: 1)')
    
    # Simulation parameters
    parser.add_argument('--drop-rate', type=float, default=0.05,
                        help='Packet drop simulation rate (0.0-1.0, default: 0.05)')
    parser.add_argument('--latency-min', type=float, default=0.0,
                        help='Minimum artificial latency in ms (default: 0.0)')
    parser.add_argument('--latency-max', type=float, default=5.0,
                        help='Maximum artificial latency in ms (default: 5.0)')
    
    # Data sources
    parser.add_argument('--use-coinbase', action='store_true',
                        help='Use Coinbase as a data source')
    parser.add_argument('--use-yahoo', action='store_true',
                        help='Use Yahoo Finance as a data source')
    parser.add_argument('--use-simulator', action='store_true',
                        help='Use simulated data source')
    
    # Other options
    parser.add_argument('--stats-interval', type=int, default=10,
                        help='Statistics display interval in seconds (default: 10)')
    
    args = parser.parse_args()
    return args


def main():
    """Main entry point"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Create publisher
    publisher = MarketDataPublisher(
        mcast_grp=args.group,
        port=args.port,
        ttl=args.ttl,
        drop_rate=args.drop_rate,
        latency_min=args.latency_min,
        latency_max=args.latency_max,
        stats_interval=args.stats_interval
    )
    
    # Create connector manager
    manager = ConnectorManager()
    
    # Add requested connectors
    if args.use_coinbase:
        crypto_connector = CoinbaseConnector(
            symbols=["BTC", "ETH", "SOL", "DOGE", "ADA"],
            output_queue=manager.get_data_queue()
        )
        manager.add_connector(crypto_connector)
    
    if args.use_yahoo:
        stock_connector = YahooFinanceConnector(
            symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"],
            output_queue=manager.get_data_queue()
        )
        manager.add_connector(stock_connector)
    
    # Always add simulator as fallback or if explicitly requested
    if args.use_simulator or not (args.use_coinbase or args.use_yahoo):
        sim_connector = SimulatedExchangeConnector(
            symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BTC", "ETH", "IBM", "NFLX"],
            output_queue=manager.get_data_queue()
        )
        manager.add_connector(sim_connector)
    
    try:
        # Start all connectors
        manager.start_all()
        
        # Start publisher (this will block until stopped)
        publisher.start(manager)
        
    except KeyboardInterrupt:
        console.print("[yellow]Shutting down...[/yellow]")
    finally:
        # Stop everything
        publisher.stop()
        manager.stop_all()
        console.print("[bold green]Market data publisher shutdown complete[/bold green]")


if __name__ == "__main__":
    main()