# Equal-Weighted Stock Index Service

A backend service that tracks and manages a custom equal-weighted stock index of the top 100 US stocks by market capitalization.

## Project Structure
```
IndexDev/
├── app/
│   ├── config.py
│   ├── main.py
│   ├── cache/
│   │   └── cache.py
│   ├── database/
│   │   ├── database.py
│   │   └── init_db.py
│   ├── routers/
│   │   └── index_router.py
│   └── services/
│       ├── data_acquisition.py
│       ├── index_calculator.py
│       └── scheduler.py
├── docker-compose.yml
├── docker-entrypoint.sh
├── Dockerfile.api
├── Dockerfile.scheduler
├── requirements.txt
└── README.md
```

## Setup Instructions

### Quick Start (Recommended)

1. Make sure Docker and Docker Compose are installed
2. Create .env file with your API keys:
```
ALPHA_VANTAGE_API_KEY=your_key_here
```

3. Build and start all services:
```bash
docker-compose up --build
```

This single command will:
- Build and start the FastAPI service
- Build and start the Scheduler service
- Start the Redis cache service
- Set up all necessary networking between services
- Mount the required volumes for persistent data
- in dockerfile.scheduler, you can add a initialize flag if you want to populate the data for last 30 days on first run
To run in detached mode:
```bash
docker-compose up -d --build
```

To stop all services:
```bash
docker-compose down
```

### Local Development Setup (Optional)

If you need to develop or debug locally without Docker, follow these steps:


1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a .env file with your API keys as shown above

3. Initialize the database:
```bash
python -m app.database.init_db
```

4. Start the FastAPI server:
```bash
uvicorn app.main:app --reload
```

Note: When running locally, you'll need to set up Redis separately if you need caching functionality.

## Running Data Acquisition

### Local Environment
To run the data acquisition job manually:
```bash
python -m app.services.data_acquisition
```

### Docker Environment
The scheduler service automatically runs data acquisition jobs based on the configured schedule. 
To manually trigger data acquisition in Docker:
```bash
docker-compose exec api python -m app.services.scheduler
```
Evidence:

Build Index:
<img width="1313" height="837" alt="image" src="https://github.com/user-attachments/assets/796a7673-b94d-4a25-87b7-741b62d85876" />

Index Composition:
<img width="1319" height="854" alt="image" src="https://github.com/user-attachments/assets/76c42904-ec6d-4349-8f6c-ef6fb669e4f1" />

Composition Changes:
<img width="1415" height="895" alt="image" src="https://github.com/user-attachments/assets/85e69e02-b981-4460-b4a1-fe4b7098465e" />

Index Performance:
![Uploading image.png…]()

Export Date:
![Uploading image.png…]()


## Database Schema

### Tables

1. stocks
   - symbol (VARCHAR, PK)
   - company_name (VARCHAR)
   - sector (VARCHAR)

2. daily_data
   - symbol (VARCHAR)
   - date (DATE)
   - price (DECIMAL)
   - market_cap (DECIMAL)
   - volume (BIGINT)
   - PRIMARY KEY (symbol, date)

3. index_composition
   - date (DATE)
   - symbol (VARCHAR)
   - weight (DECIMAL)
   - rank (INT)
   - PRIMARY KEY (date, symbol)

4. index_performance
   - date (DATE, PK)
   - daily_return (DECIMAL)
   - cumulative_return (DECIMAL)

Evidence:

Build Index:
<img width="1313" height="837" alt="image" src="https://github.com/user-attachments/assets/796a7673-b94d-4a25-87b7-741b62d85876" />

Index Composition:
<img width="1319" height="854" alt="image" src="https://github.com/user-attachments/assets/76c42904-ec6d-4349-8f6c-ef6fb669e4f1" />

Composition Changes:
<img width="1415" height="895" alt="image" src="https://github.com/user-attachments/assets/85e69e02-b981-4460-b4a1-fe4b7098465e" />

Index Performance:
![Uploading image.png…]()

Export Date:
![Uploading image.png…]()




## API Usage

### Build Index
```bash
curl -X POST "http://localhost:8000/build-index?start_date=2023-10-01&end_date=2023-10-31"
```

### Get Index Performance
```bash
curl "http://localhost:8000/index-performance?start_date=2023-10-01&end_date=2023-10-31"
```

### Get Index Composition
```bash
curl "http://localhost:8000/index-composition?date=2023-10-01"
```

### Get Composition Changes
```bash
curl "http://localhost:8000/composition-changes?start_date=2023-10-01&end_date=2023-10-31"
```

### Export Data
```bash
curl -X POST "http://localhost:8000/export-data" --output index_data.xlsx"
```

## Postman Collection
You can import the following curl commands into Postman:

```json
{
  "info": {
    "name": "Stock Index API",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "Build Index",
      "request": {
        "method": "POST",
        "url": "http://localhost:8000/build-index",
        "query": [
          {"key": "start_date", "value": "2023-10-01"},
          {"key": "end_date", "value": "2023-10-31"}
        ]
      }
    },
    {
      "name": "Get Index Performance",
      "request": {
        "method": "GET",
        "url": "http://localhost:8000/index-performance",
        "query": [
          {"key": "start_date", "value": "2023-10-01"},
          {"key": "end_date", "value": "2023-10-31"}
        ]
      }
    },
    {
      "name": "Get Index Composition",
      "request": {
        "method": "GET",
        "url": "http://localhost:8000/index-composition",
        "query": [
          {"key": "date", "value": "2023-10-01"}
        ]
      }
    }
  ]
}
```

## Production & Scaling Recommendations
- Add indexes for frequently queried columns (date, symbol, market_cap)
- Partition daily_data table by date ranges
- Implement query result caching at database level
- Use cache more effeciently by using keys for cumulative queries
- Implement Redis cluster mode for horizontal scaling
- Add Redis Sentinel for high availability
- Implement cache warming for common date ranges
- Implement fallback cache strategy
- Load Balance requests across multiple API instances
- Add circuit breakers for external API calls
- Implement request rate limiting and queueing
- Separate scheduler into standalone microservice
- Shard the tables into different instances for fast query
- Implement batch processing for large date ranges
- Implement parallel processing for index calculations
- Add monitoring for all the services and raise timely alets using prometheus, grafana and alert manager
