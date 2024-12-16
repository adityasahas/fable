# FABLE: Finding Aliases for Broken Links Efficiently

This is the repository for the paper **Reviving Dead Links on the Web with FABLE** published in IMC 2023. The paper can be found [here](https://dl-acm-org.proxy.lib.umich.edu/doi/10.1145/3618257.3624832).

## Setup and Installation

### Prerequisites
- Docker
- Docker Compose

### Installation

1. Clone the repository:
```bash
git clone repourl
cd repo
```

2. Configuration

Before running, create a `config.json` file in `/fable/config.json` with the following required values:

```json
{
    "mongo_hostname": "your_mongodb_host",
    "mongo_user": "your_mongodb_user",
    "mongo_pwd": "your_mongodb_password",
    "mongo_db": "fable",
    "mongo_url": "your_mongodb_connection_url",  // Optional: alternative to hostname/user/pwd
    "proxies": ["proxy1", "proxy2"],  // Optional: list of proxy servers
    "tmp_path": "./tmp",  // Optional: defaults to ./tmp
    "localserver_port": 24680  // Optional: defaults to 24680
}
```

3. Build and run using Docker Compose:
```bash
docker-compose up --build
```

Or run in detached mode:
```bash
docker-compose up -d --build
```

### Docker Commands

Start the service:
```bash
docker-compose up
```

Stop the service:
```bash
docker-compose down
```

View logs while running in detached mode:
```bash
docker-compose logs -f
```

### Resource Configuration

The service is configured to use:
- Memory: 8GB
- CPUs: 2 cores

These settings can be modified in the `docker-compose.yml` file.

### Logging

Logs are automatically saved to the `/logs` directory in your project root. The logging system creates timestamped files in the format:
```
logs/fable_api_YYYYMMDD_HHMMSS.txt
```

## API Endpoints

Documentation can be found at
https://fablee.vercel.app