## Prerequisites

Before you begin, ensure you have the following installed:
- Python 3.10 or later
- JDK (Java Development Kit)

### Install JDK

For Arch Linux and its derivatives:

```bash
sudo pacman -S jdk-openjdk
```
### Install Poetry
You can install Poetry using pip:

```bash
pip install poetry
```

Or you can use the installer script:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```
- Configure Poetry to create virtual environments within the project directory:

```bash
poetry config virtualenvs.in-project true
```

### Install Dependencies
Navigate to your project directory and install dependencies:


```bash
cd getprog
poetry install
```

### Activate Virtual Environment
Activate the Poetry-created virtual environment:

```bash
source .venv/bin/activate
```

### Install and Run Cassandra
Pull the Cassandra Docker image:


```bash
docker pull cassandra:latest
```

Run a Cassandra container:

```bash
docker run --name my-cassandra -p 9042:9042 -d cassandra:latest
```

#### Connect to the Cassandra container:


```bash
docker exec -it <container-id> bash
```
Replace <container-id> with the ID of the Cassandra container.

- Connect to Cassandra using CQLSH:

```bash
cqlsh
```

- Create a keyspace:

```bash
CREATE KEYSPACE getprog_ia WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

Check the keyspace creation:

```bash
DESCRIBE KEYSPACES;
```

### Run Scripts


```bash
python scraper.py
```

















