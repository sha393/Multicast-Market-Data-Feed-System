# Multicast-Market-Data-Feed-System

A comprehensive real-time market data distribution system that simulates financial exchange data feeds using multicast networking. Capable of connecting to real cryptocurrency exchanges for live data or generating simulated market data.



![System Dashboard](https://raw.githubusercontent.com/sha393/Multicast-Market-Data-Feed-System/main/market_data_demo.png)



## Key Features

- **Real-time market data distribution** using UDP multicast for efficient one-to-many delivery
- **Exchange connectors** for Coinbase (cryptocurrency) and Yahoo Finance (stocks)
- **Dynamic data visualization** with real-time price charts and statistics
- **Sequence validation** to ensure data integrity and completeness
- **Performance metrics** including latency measurement and throughput tracking
- **Data analysis tools** for post-processing and visualization of market data feeds
- **Simulated trading** with a simple moving average crossover strategy

## System Architecture

```
┌─────────────────┐       ┌─────────────────────┐       ┌─────────────────┐
│  Exchange       │──────▶│  Multicast          │──────▶│  Multiple       │
│  Connectors     │       │  Publisher          │       │  Receivers      │
└─────────────────┘       └─────────────────────┘       └─────────────────┘
      │                          │                           │
      │                          │                           │
      ▼                          ▼                           ▼
┌─────────────────┐       ┌─────────────────────┐       ┌─────────────────┐
│  Market Data    │       │  Network            │       │  Data Logging   │
│  Sources        │       │  Transport          │       │  & Analysis     │
└─────────────────┘       └─────────────────────┘       └─────────────────┘
```

## Requirements

- Python 3.8+
- Required packages:
  - rich (for UI components)
  - matplotlib (for data visualization)
  - numpy and pandas (for data analysis)
  - websockets (for real-time exchange connections)
  - requests (for API requests)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/market-data-feed-system.git
   cd market-data-feed-system
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements_updated.txt
   ```

## Usage

### Publisher (Market Data Source)

Start the market data publisher to broadcast market data:

```bash
# Using simulated data
python real_multicast_sender.py --use-simulator

# Using real cryptocurrency data from Coinbase
python real_multicast_sender.py --use-coinbase

# Using real stock market data (during market hours)
python real_multicast_sender.py --use-yahoo

# Using both real data sources
python real_multicast_sender.py --use-coinbase --use-yahoo
```

Additional publisher options:
```bash
# Set network parameters
python real_multicast_sender.py --use-simulator --group 224.1.1.1 --port 5007 --ttl 1

# Configure simulation parameters
python real_multicast_sender.py --use-simulator --drop-rate 0.01 --latency-min 0.1 --latency-max 2.0
```

### Receiver (Market Data Consumer)

Run one or more receivers to listen for market data:

```bash
# Start receiver with default settings
python real_multicast_receiver.py

# Specify display mode
python real_multicast_receiver.py --display full   # Options: full, prices, stats, portfolio

# Enable verbose output
python real_multicast_receiver.py --verbose
```

### Data Analysis

Analyze collected market data logs:

```bash
# Analyze the most recent log file
python market_data_analyzer.py --latest

# List available log files
python market_data_analyzer.py --list

# Analyze a specific file
python market_data_analyzer.py --file logs/market_data_20250511_103215.csv

# Analyze all log files
python market_data_analyzer.py --all
```

## Analysis and Visualization

The system provides comprehensive analytics for market data feeds:

- **Latency Analysis**: Histograms and time series of message latencies
- **Message Rate**: Real-time and historical message throughput
- **Price Movements**: Charts showing price trends for each symbol
- **Data Sources**: Distribution of messages by exchange
- **Network Performance**: Sequence gap detection and analysis

## Components

### Exchange Connector (`exchange_connector.py`)
Connects to market data sources (real or simulated) and provides a unified interface for market data.

Key Features:
- Multiple exchange support (Coinbase, Yahoo Finance, Simulated)
- Fault tolerance with automatic reconnection
- Standardized message format

### Market Data Publisher (`real_multicast_sender.py`)
Publishes market data to a multicast group with configurable parameters.

Key Features:
- Multicast UDP distribution
- Configurable publishing parameters
- Real-time statistics

### Market Data Receiver (`real_multicast_receiver.py`)
Receives and processes market data with a rich dashboard interface.

Key Features:
- Sequence validation
- Latency measurement
- Rich data visualization
- Trading simulation

### Market Data Analyzer (`market_data_analyzer.py`)
Performs post-processing analysis on log files.

Key Features:
- Advanced visualizations
- Statistical analysis
- Performance recommendations

## Best Practices

For production use, consider:

- Implementing redundant data feeds
- Using higher-performance network equipment
- Adding dedicated sequence validation and recovery
- Setting up real-time monitoring and alerting
- Implementing proper clock synchronization for accurate latency measurement

## Educational Value

This system demonstrates:
- Real-time data distribution architecture
- Financial market data processing
- Network programming with UDP multicast
- Real-time visualization techniques
- Performance measurement methodology
- Data quality validation

## License

MIT License

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.
