# ordershop
This shows Redis as a communication channel in a microservices architecture.

## Disclaimer
*This is not production ready code, this is a sample implementation for showcasing only!*

## Prerequisites

- Python v3
- Docker Compose v1.25 (or Kubernetes v1.17)

## Start

- `docker-compose up`

## Test

- `python3 -m unittest tests/unit.py`

## View

- Go to `http://localhost:5000/` for watching events and browsing state.

## Stop

- `docker-compose down`
