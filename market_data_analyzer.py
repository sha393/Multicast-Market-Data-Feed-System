#!/usr/bin/env python3
"""
Market Data Feed Analyzer

Analyzes log files from the market data receiver to provide insights on:
- Network performance (latency, packet loss)
- Data quality (gaps, outliers)
- Market statistics
- Visualization of price/volume data
"""
import os
import sys
import csv
import json
import glob
import argparse
import datetime
import statistics
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import PercentFormatter
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track
from rich.layout import Layout
from rich.markdown import Markdown



# Create logs and analysis directories if they don't exist
os.makedirs('logs', exist_ok=True)
os.makedirs('analysis', exist_ok=True)

# Configure rich console
console = Console()

class MarketDataAnalyzer:
    """Analyzes market data log files"""
    
    def __init__(self, log_file=None, output_dir=None):
        self.log_file = log_file
        self.output_dir = output_dir or "analysis"
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize data structures
        self.data = None
        self.summary = {}
        self.latency_stats = {}
        self.exchange_stats = {}
        self.symbol_stats = {}
        self.network_stats = {}
        self.last_stats_time = None  # Initialize the missing variable
        
        console.print(Panel.fit(
            f"[bold cyan]Market Data Feed Analyzer[/bold cyan]\n"
            f"Analyzing: [green]{log_file or 'No file selected yet'}[/green]\n"
            f"Output directory: [green]{self.output_dir}[/green]"
        ))
    
    def load_data(self, log_file=None):
        """Load data from a log file"""
        if log_file:
            self.log_file = log_file
            
        if not self.log_file:
            console.print("[red]Error: No log file specified[/red]")
            return False
            
        console.print(f"Loading data from [cyan]{self.log_file}[/cyan]...")
        
        try:
            # Read CSV file into pandas DataFrame
            self.data = pd.read_csv(self.log_file, parse_dates=['timestamp'])
            
            # Convert price and volume to numeric
            self.data['price'] = pd.to_numeric(self.data['price'], errors='coerce')
            self.data['volume'] = pd.to_numeric(self.data['volume'], errors='coerce')
            self.data['latency_ms'] = pd.to_numeric(self.data['latency_ms'], errors='coerce')
            self.data['msg_id'] = pd.to_numeric(self.data['msg_id'], errors='coerce')
            
            # Calculate derived fields
            self.data['time_delta'] = self.data['timestamp'].diff().dt.total_seconds()
            
            # Print basic info
            console.print(f"Loaded [green]{len(self.data)}[/green] records covering " +
                         f"[green]{self.data['timestamp'].max() - self.data['timestamp'].min()}[/green]")
            
            return True
            
        except Exception as e:
            console.print(f"[red]Error loading data: {str(e)}[/red]")
            return False
    
    def analyze_data(self):
        """Analyze the loaded data"""
        if self.data is None or len(self.data) == 0:
            console.print("[red]No data to analyze[/red]")
            return False
            
        console.print("Analyzing market data...")
        
        # Generate basic summary
        self.summary = {
            'record_count': len(self.data),
            'start_time': self.data['timestamp'].min(),
            'end_time': self.data['timestamp'].max(),
            'duration_seconds': (self.data['timestamp'].max() - self.data['timestamp'].min()).total_seconds(),
            'unique_symbols': self.data['symbol'].nunique(),
            'unique_exchanges': self.data['exchange'].nunique()
        }
        
        # Calculate messages per second
        self.summary['messages_per_second'] = self.summary['record_count'] / max(self.summary['duration_seconds'], 1)
        
        # Analyze latency
        latency_data = self.data['latency_ms'].dropna()
        if len(latency_data) > 0:
            self.latency_stats = {
                'min': latency_data.min(),
                'max': latency_data.max(),
                'mean': latency_data.mean(),
                'median': latency_data.median()
            }
            
            # Calculate percentiles if we have enough data
            if len(latency_data) >= 100:
                self.latency_stats['p95'] = latency_data.quantile(0.95)
                self.latency_stats['p99'] = latency_data.quantile(0.99)
                
            # Calculate standard deviation and jitter
            self.latency_stats['stddev'] = latency_data.std()
            self.latency_stats['jitter'] = self.data['latency_ms'].diff().abs().mean()
        
        # Analyze exchanges
        exchange_counts = self.data['exchange'].value_counts()
        self.exchange_stats = {
            'counts': exchange_counts.to_dict(),
            'percentages': (exchange_counts / len(self.data) * 100).to_dict()
        }
        
        # Analyze symbols
        # Group by symbol and calculate statistics
        symbol_groups = self.data.groupby('symbol')
        
        self.symbol_stats = {}
        for symbol, group in track(symbol_groups, description="Analyzing symbols..."):
            self.symbol_stats[symbol] = {
                'count': len(group),
                'min_price': group['price'].min(),
                'max_price': group['price'].max(),
                'mean_price': group['price'].mean(),
                'price_range_pct': (group['price'].max() - group['price'].min()) / group['price'].min() * 100 if group['price'].min() > 0 else 0,
                'volatility': group['price'].pct_change().std() * 100,  # Annualized volatility would be * sqrt(252)
                'volume': group['volume'].sum()
            }
            
            # Calculate price velocity (rate of change)
            if len(group) >= 2:
                price_changes = group['price'].diff().dropna()
                time_diffs = group['timestamp'].diff().dt.total_seconds().dropna()
                
                if len(price_changes) > 0 and len(time_diffs) > 0:
                    price_velocity = price_changes / time_diffs
                    self.symbol_stats[symbol]['price_velocity'] = price_velocity.mean()
                    self.symbol_stats[symbol]['price_acceleration'] = price_velocity.diff().mean()
        
        # Analyze network performance
        msg_ids = self.data['msg_id'].dropna()
        if len(msg_ids) > 0:
            # Sort by msg_id to analyze sequence
            sequence_data = self.data.dropna(subset=['msg_id']).sort_values('msg_id')
            
            # Calculate gaps in sequence
            sequence_data['id_diff'] = sequence_data['msg_id'].diff()
            gaps = sequence_data[sequence_data['id_diff'] > 1]
            
            total_missing = gaps['id_diff'].sum() - len(gaps) if len(gaps) > 0 else 0
            
            self.network_stats = {
                'sequence_gaps_count': len(gaps),
                'missing_messages': total_missing,
                'drop_rate': total_missing / (max(msg_ids) - min(msg_ids) + 1) if len(msg_ids) > 0 else 0,
                'effective_bandwidth_kbps': None  # Will calculate if we have time data
            }
            
            # Calculate processing time differences and bandwidth if we have timestamps
            if 'time_delta' in self.data and not self.data['time_delta'].isna().all():
                # Calculate average bytes per message (assume JSON overhead)
                avg_bytes_per_msg = 200  # Rough estimate for JSON message size
                
                # Calculate effective bandwidth in kbps
                total_bytes = avg_bytes_per_msg * len(self.data)
                duration_sec = self.summary['duration_seconds']
                
                if duration_sec > 0:
                    self.network_stats['effective_bandwidth_kbps'] = (total_bytes * 8 / 1000) / duration_sec
        
        console.print("[green]Analysis complete![/green]")
        return True
    
    def generate_visualizations(self):
        """Generate visualizations from the analyzed data"""
        if self.data is None or len(self.data) == 0:
            console.print("[red]No data to visualize[/red]")
            return False
            
        console.print("Generating visualizations...")
        
        # Extract the base file name without extension for output files
        base_name = os.path.splitext(os.path.basename(self.log_file))[0]
        
        # 1. Latency distribution histogram
        if 'latency_ms' in self.data.columns and not self.data['latency_ms'].isna().all():
            plt.figure(figsize=(10, 6))
            plt.hist(self.data['latency_ms'].dropna(), bins=50, alpha=0.7, color='blue', edgecolor='black')
            
            # Add lines for mean and median
            if 'mean' in self.latency_stats:
                plt.axvline(self.latency_stats['mean'], color='red', linestyle='dashed', 
                            linewidth=1, label=f"Mean: {self.latency_stats['mean']:.2f}ms")
            if 'median' in self.latency_stats:
                plt.axvline(self.latency_stats['median'], color='green', linestyle='dashed', 
                            linewidth=1, label=f"Median: {self.latency_stats['median']:.2f}ms")
            if 'p95' in self.latency_stats:
                plt.axvline(self.latency_stats['p95'], color='orange', linestyle='dashed', 
                            linewidth=1, label=f"95th: {self.latency_stats['p95']:.2f}ms")
            
            plt.title('Message Latency Distribution')
            plt.xlabel('Latency (ms)')
            plt.ylabel('Frequency')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Save figure
            latency_file = os.path.join(self.output_dir, f"{base_name}_latency_histogram.png")
            plt.savefig(latency_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            console.print(f"Saved latency histogram to [cyan]{latency_file}[/cyan]")
            
            # 1b. Latency over time
            plt.figure(figsize=(12, 6))
            plt.plot(self.data['timestamp'], self.data['latency_ms'], 'b-', alpha=0.5)
            plt.plot(self.data['timestamp'], self.data['latency_ms'].rolling(window=20).mean(), 'r-', 
                    linewidth=2, label='20-sample Moving Average')
            
            plt.title('Message Latency Over Time')
            plt.xlabel('Time')
            plt.ylabel('Latency (ms)')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Format x-axis to show time properly
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            plt.gcf().autofmt_xdate()
            
            # Save figure
            latency_time_file = os.path.join(self.output_dir, f"{base_name}_latency_time.png")
            plt.savefig(latency_time_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            console.print(f"Saved latency-over-time plot to [cyan]{latency_time_file}[/cyan]")
        
        # 2. Price charts for top symbols
        top_symbols = sorted(self.symbol_stats.keys(), 
                           key=lambda s: self.symbol_stats[s]['count'],
                           reverse=True)[:5]  # Top 5 symbols by message count
        
        for symbol in track(top_symbols, description="Generating price charts..."):
            # Filter data for this symbol
            symbol_data = self.data[self.data['symbol'] == symbol].sort_values('timestamp')
            
            if len(symbol_data) < 2:
                continue
                
            plt.figure(figsize=(12, 8))
            
            # Create two subplots
            ax1 = plt.subplot(2, 1, 1)  # Price chart
            ax2 = plt.subplot(2, 1, 2)  # Volume chart
            
            # Plot price
            ax1.plot(symbol_data['timestamp'], symbol_data['price'], 'b-')
            ax1.set_title(f'Price Chart for {symbol}')
            ax1.set_ylabel('Price ($)')
            ax1.grid(True, alpha=0.3)
            
            # Format x-axis to show time properly
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            
            # Plot volume if available
            if 'volume' in symbol_data and not symbol_data['volume'].isna().all():
                ax2.bar(symbol_data['timestamp'], symbol_data['volume'], color='green', alpha=0.6)
                ax2.set_title(f'Volume Chart for {symbol}')
                ax2.set_xlabel('Time')
                ax2.set_ylabel('Volume')
                ax2.grid(True, alpha=0.3)
                
                # Format x-axis to show time properly
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            
            plt.tight_layout()
            
            # Save figure
            price_file = os.path.join(self.output_dir, f"{base_name}_{symbol}_price_volume.png")
            plt.savefig(price_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            console.print(f"Saved price chart for {symbol} to [cyan]{price_file}[/cyan]")
        
        # 3. Exchange distribution pie chart
        if self.exchange_stats and 'counts' in self.exchange_stats:
            plt.figure(figsize=(10, 8))
            plt.pie(self.exchange_stats['counts'].values(), 
                   labels=self.exchange_stats['counts'].keys(),
                   autopct='%1.1f%%',
                   startangle=90,
                   shadow=True)
            plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
            plt.title('Market Data by Exchange')
            
            # Save figure
            exchange_file = os.path.join(self.output_dir, f"{base_name}_exchange_distribution.png")
            plt.savefig(exchange_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            console.print(f"Saved exchange distribution chart to [cyan]{exchange_file}[/cyan]")
        
        # 4. Message rate over time
        plt.figure(figsize=(12, 6))
        
        # Create time buckets (e.g., 1-second intervals)
        self.data['time_bucket'] = self.data['timestamp'].dt.floor('1S')
        message_rate = self.data.groupby('time_bucket').size()
        
        plt.plot(message_rate.index, message_rate.values)
        plt.title('Message Rate Over Time')
        plt.xlabel('Time')
        plt.ylabel('Messages per Second')
        plt.grid(True, alpha=0.3)
        
        # Format x-axis to show time properly
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.gcf().autofmt_xdate()
        
        # Save figure
        rate_file = os.path.join(self.output_dir, f"{base_name}_message_rate.png")
        plt.savefig(rate_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        console.print(f"Saved message rate chart to [cyan]{rate_file}[/cyan]")
        
        # 5. Missing sequence numbers visualization
        if self.network_stats and 'sequence_gaps_count' in self.network_stats and self.network_stats['sequence_gaps_count'] > 0:
            # Sort by msg_id
            sequence_data = self.data.dropna(subset=['msg_id']).sort_values('msg_id')
            
            # Calculate gaps
            sequence_data['id_diff'] = sequence_data['msg_id'].diff()
            gaps = sequence_data[sequence_data['id_diff'] > 1]
            
            if len(gaps) > 0:
                plt.figure(figsize=(12, 6))
                
                # Plot the size of each gap
                plt.bar(range(len(gaps)), gaps['id_diff'] - 1, color='red', alpha=0.7)
                
                plt.title('Missing Messages by Sequence Gap')
                plt.xlabel('Gap Index')
                plt.ylabel('Number of Missing Messages')
                plt.grid(True, alpha=0.3)
                
                # Save figure
                gaps_file = os.path.join(self.output_dir, f"{base_name}_sequence_gaps.png")
                plt.savefig(gaps_file, dpi=300, bbox_inches='tight')
                plt.close()
                
                console.print(f"Saved sequence gaps chart to [cyan]{gaps_file}[/cyan]")
                
                # 5b. Distribution of missing messages over time
                plt.figure(figsize=(12, 6))
                
                plt.plot(gaps['timestamp'], gaps['id_diff'] - 1, 'ro-')
                
                plt.title('Missing Messages Over Time')
                plt.xlabel('Time')
                plt.ylabel('Number of Missing Messages')
                plt.grid(True, alpha=0.3)
                
                # Format x-axis to show time properly
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
                plt.gcf().autofmt_xdate()
                
                # Save figure
                gaps_time_file = os.path.join(self.output_dir, f"{base_name}_gaps_time.png")
                plt.savefig(gaps_time_file, dpi=300, bbox_inches='tight')
                plt.close()
                
                console.print(f"Saved gaps-over-time chart to [cyan]{gaps_time_file}[/cyan]")
        
        console.print("[green]Visualizations complete![/green]")
        return True
    
    def generate_report(self):
        """Generate a markdown report with the analysis results"""
        if not self.summary or not self.latency_stats:
            console.print("[red]No analysis results to report[/red]")
            return False
            
        console.print("Generating markdown report...")
        
        # Extract the base file name without extension for output files
        base_name = os.path.splitext(os.path.basename(self.log_file))[0]
        report_file = os.path.join(self.output_dir, f"{base_name}_analysis_report.md")
        
        # Create markdown content
        markdown = f"""# Market Data Feed Analysis Report

## Overview

- **Data Source**: `{self.log_file}`
- **Analysis Time**: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Records Analyzed**: {self.summary.get('record_count', 0):,}
- **Time Period**: {self.summary.get('start_time')} to {self.summary.get('end_time')}
- **Duration**: {self.summary.get('duration_seconds', 0):.2f} seconds
- **Message Rate**: {self.summary.get('messages_per_second', 0):.2f} messages/second

## Network Performance

| Metric | Value |
|--------|-------|
| Sequence Gaps | {self.network_stats.get('sequence_gaps_count', 'N/A')} |
| Missing Messages | {self.network_stats.get('missing_messages', 'N/A')} |
| Drop Rate | {self.network_stats.get('drop_rate', 0) * 100:.2f}% |
| Effective Bandwidth | {self.network_stats.get('effective_bandwidth_kbps', 'N/A')} kbps |

### Latency Statistics

| Metric | Value (ms) |
|--------|------------|
| Minimum | {self.latency_stats.get('min', 'N/A'):.2f} |
| Maximum | {self.latency_stats.get('max', 'N/A'):.2f} |
| Mean | {self.latency_stats.get('mean', 'N/A'):.2f} |
| Median (P50) | {self.latency_stats.get('median', 'N/A'):.2f} |"""

        # Add percentiles if available
        if 'p95' in self.latency_stats:
            markdown += f"\n| 95th Percentile | {self.latency_stats.get('p95', 'N/A'):.2f} |"
        if 'p99' in self.latency_stats:
            markdown += f"\n| 99th Percentile | {self.latency_stats.get('p99', 'N/A'):.2f} |"
            
        markdown += f"\n| Standard Deviation | {self.latency_stats.get('stddev', 'N/A'):.2f} |"
        markdown += f"\n| Jitter | {self.latency_stats.get('jitter', 'N/A'):.2f} |"

        # Add visualizations
        markdown += f"""

### Latency Distribution
![Latency Histogram](./{os.path.basename(self.output_dir)}/{base_name}_latency_histogram.png)

### Latency Over Time
![Latency Over Time](./{os.path.basename(self.output_dir)}/{base_name}_latency_time.png)

### Message Rate
![Message Rate](./{os.path.basename(self.output_dir)}/{base_name}_message_rate.png)

"""

        # Add sequence gaps charts if available
        if self.network_stats.get('sequence_gaps_count', 0) > 0:
            markdown += f"""
### Sequence Gaps
![Sequence Gaps](./{os.path.basename(self.output_dir)}/{base_name}_sequence_gaps.png)

### Gaps Over Time
![Gaps Over Time](./{os.path.basename(self.output_dir)}/{base_name}_gaps_time.png)
"""

        # Add exchange distribution
        if self.exchange_stats and 'counts' in self.exchange_stats:
            markdown += f"""
## Data Sources

### Exchange Distribution
![Exchange Distribution](./{os.path.basename(self.output_dir)}/{base_name}_exchange_distribution.png)

| Exchange | Messages | Percentage |
|----------|----------|------------|
"""
            for exchange, count in self.exchange_stats['counts'].items():
                percentage = self.exchange_stats['percentages'][exchange]
                markdown += f"| {exchange} | {count:,} | {percentage:.2f}% |\n"

        # Add symbol statistics
        if self.symbol_stats:
            markdown += f"""
## Symbol Analysis

| Symbol | Count | Min Price | Max Price | Range % | Volatility | Volume |
|--------|-------|-----------|-----------|---------|------------|--------|
"""
            # Sort symbols by message count
            sorted_symbols = sorted(self.symbol_stats.keys(), 
                                  key=lambda s: self.symbol_stats[s]['count'],
                                  reverse=True)
                                  
            for symbol in sorted_symbols[:10]:  # Top 10 symbols
                stats = self.symbol_stats[symbol]
                markdown += (f"| {symbol} | {stats['count']:,} | "
                           f"${stats['min_price']:.2f} | ${stats['max_price']:.2f} | "
                           f"{stats['price_range_pct']:.2f}% | {stats['volatility']:.2f}% | "
                           f"{stats.get('volume', 'N/A'):,} |\n")

            # Add price charts for top symbols
            markdown += f"\n## Price Charts\n\n"
            
            for symbol in sorted_symbols[:5]:  # Top 5 symbols by message count
                markdown += f"### {symbol}\n"
                markdown += f"![{symbol} Price and Volume](./{os.path.basename(self.output_dir)}/{base_name}_{symbol}_price_volume.png)\n\n"

        # Add recommendations
        markdown += f"""
## Recommendations

Based on the analysis of this market data feed, here are some recommendations:

"""
        # Add data-driven recommendations
        if self.latency_stats.get('mean', 0) > 20:
            markdown += "- **High Latency**: The average latency ({:.2f}ms) is high. Consider optimizing the network path or reducing artificial latency.\n".format(self.latency_stats['mean'])
            
        if self.latency_stats.get('jitter', 0) > 10:
            markdown += "- **High Jitter**: Latency variation ({:.2f}ms) is significant. This could impact time-sensitive trading strategies.\n".format(self.latency_stats['jitter'])
            
        if self.network_stats.get('drop_rate', 0) > 0.05:
            markdown += "- **Packet Loss**: The drop rate ({:.2f}%) exceeds 5%. Consider investigating network congestion or buffer configurations.\n".format(self.network_stats['drop_rate'] * 100)
            
        # Add general recommendations
        markdown += """
- Consider implementing a redundant multicast feed to mitigate data loss.
- For trading strategies, implement proper sequence validation and gap detection.
- Monitor latency trends over longer periods to identify patterns or degradation.
"""

        # Save the markdown report
        with open(report_file, 'w') as f:
            f.write(markdown)
            
        console.print(f"Saved analysis report to [cyan]{report_file}[/cyan]")
        return True
    
    def find_log_files(self, directory="logs"):
        """Find all log files in the specified directory"""
        pattern = os.path.join(directory, "market_data_*.csv")
        files = glob.glob(pattern)
        return sorted(files, key=os.path.getmtime, reverse=True)  # Sort by modification time
    
    def print_summary(self):
        """Print a summary of the analysis results"""
        if not self.summary or not self.latency_stats:
            console.print("[red]No analysis results to summarize[/red]")
            return
            
        # Create summary table
        summary_table = Table(title="Market Data Feed Analysis Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        # Basic info
        summary_table.add_row("Records Analyzed", f"{self.summary.get('record_count', 0):,}")
        summary_table.add_row("Time Period", f"{self.summary.get('start_time')} to {self.summary.get('end_time')}")
        summary_table.add_row("Duration", f"{self.summary.get('duration_seconds', 0):.2f} seconds")
        summary_table.add_row("Message Rate", f"{self.summary.get('messages_per_second', 0):.2f} msgs/sec")
        summary_table.add_row("Unique Symbols", f"{self.summary.get('unique_symbols', 0)}")
        summary_table.add_row("Unique Exchanges", f"{self.summary.get('unique_exchanges', 0)}")
        
        # Network stats
        summary_table.add_row("", "")
        summary_table.add_row("[bold]Network Performance[/bold]", "")
        summary_table.add_row("Sequence Gaps", f"{self.network_stats.get('sequence_gaps_count', 'N/A')}")
        summary_table.add_row("Missing Messages", f"{self.network_stats.get('missing_messages', 'N/A')}")
        summary_table.add_row("Drop Rate", f"{self.network_stats.get('drop_rate', 0) * 100:.2f}%")
        
        # Latency stats
        summary_table.add_row("", "")
        summary_table.add_row("[bold]Latency Statistics[/bold]", "")
        summary_table.add_row("Min Latency", f"{self.latency_stats.get('min', 'N/A'):.2f} ms")
        summary_table.add_row("Avg Latency", f"{self.latency_stats.get('mean', 'N/A'):.2f} ms")
        summary_table.add_row("Median Latency", f"{self.latency_stats.get('median', 'N/A'):.2f} ms")
        summary_table.add_row("Max Latency", f"{self.latency_stats.get('max', 'N/A'):.2f} ms")
        
        if 'p95' in self.latency_stats:
            summary_table.add_row("95th Percentile", f"{self.latency_stats.get('p95'):.2f} ms")
            
        summary_table.add_row("Jitter", f"{self.latency_stats.get('jitter', 'N/A'):.2f} ms")
        
        console.print(summary_table)
        
        # Print top symbols
        if self.symbol_stats:
            # Sort symbols by message count
            sorted_symbols = sorted(self.symbol_stats.keys(), 
                                 key=lambda s: self.symbol_stats[s]['count'],
                                 reverse=True)
                                 
            symbol_table = Table(title="Top Symbols by Message Count")
            symbol_table.add_column("Symbol", style="cyan")
            symbol_table.add_column("Count", style="green", justify="right")
            symbol_table.add_column("Min Price", style="yellow", justify="right")
            symbol_table.add_column("Max Price", style="yellow", justify="right")
            symbol_table.add_column("Volatility", style="magenta", justify="right")
            
            for symbol in sorted_symbols[:10]:  # Top 10 symbols
                stats = self.symbol_stats[symbol]
                symbol_table.add_row(
                    symbol,
                    f"{stats['count']:,}",
                    f"${stats['min_price']:.2f}",
                    f"${stats['max_price']:.2f}",
                    f"{stats['volatility']:.2f}%"
                )
                
            console.print(symbol_table)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Market Data Feed Analyzer')
    
    # Input files
    parser.add_argument('--file', type=str,
                        help='Path to the log file to analyze')
    parser.add_argument('--dir', type=str, default='logs',
                        help='Directory containing log files (default: logs)')
    
    # Output options
    parser.add_argument('--output', type=str, default='analysis',
                        help='Output directory for analysis results (default: analysis)')
    
    # Analysis options
    parser.add_argument('--list', action='store_true',
                        help='List available log files and exit')
    parser.add_argument('--latest', action='store_true',
                        help='Analyze the most recent log file')
    parser.add_argument('--all', action='store_true',
                        help='Analyze all log files in the directory')
    
    args = parser.parse_args()
    return args


def main():
    """Main entry point"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Create analyzer
    analyzer = MarketDataAnalyzer(output_dir=args.output)
    
    # List log files if requested
    if args.list:
        log_files = analyzer.find_log_files(args.dir)
        
        if not log_files:
            console.print(f"[yellow]No log files found in directory: {args.dir}[/yellow]")
            return
            
        console.print(f"[green]Found {len(log_files)} log files:[/green]")
        
        file_table = Table(title=f"Log Files in {args.dir}")
        file_table.add_column("Filename", style="cyan")
        file_table.add_column("Size", style="green", justify="right")
        file_table.add_column("Modified", style="yellow")
        
        for file_path in log_files:
            # Get file stats
            stats = os.stat(file_path)
            size = stats.st_size / 1024  # KB
            mod_time = datetime.datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            
            file_table.add_row(
                os.path.basename(file_path),
                f"{size:.1f} KB",
                mod_time
            )
            
        console.print(file_table)
        return
    
    # Determine which file(s) to analyze
    files_to_analyze = []
    
    if args.file:
        files_to_analyze.append(args.file)
    elif args.latest:
        log_files = analyzer.find_log_files(args.dir)
        if log_files:
            files_to_analyze.append(log_files[0])
        else:
            console.print(f"[red]No log files found in directory: {args.dir}[/red]")
            return
    elif args.all:
        files_to_analyze = analyzer.find_log_files(args.dir)
        if not files_to_analyze:
            console.print(f"[red]No log files found in directory: {args.dir}[/red]")
            return
    else:
        # If no file specified, show help
        console.print("[yellow]No file specified for analysis.[/yellow]")
        console.print("Use --file to specify a file, --latest to analyze the most recent log file,")
        console.print("or --list to see available log files.")
        return
    
    # Process each file
    for file_path in files_to_analyze:
        console.print(Panel.fit(f"[bold]Analyzing file:[/bold] [cyan]{file_path}[/cyan]"))
        
        # Load and analyze the data
        if analyzer.load_data(file_path) and analyzer.analyze_data():
            # Print summary
            analyzer.print_summary()
            
            # Generate visualizations
            analyzer.generate_visualizations()
            
            # Generate report
            analyzer.generate_report()
            
            console.print(f"[green]Analysis complete for {file_path}[/green]")
        else:
            console.print(f"[red]Failed to analyze {file_path}[/red]")
            
        console.print("\n")


if __name__ == "__main__":
    main()