# OrderShop v2
This shows Redis in infrastructure services in a microservice architecture.

## Disclaimer
*This is __not__ production ready code, this is a sample implementation for showcasing only!*

## Prerequisites

- Python v3.5
- Docker Compose v1.25 (or Kubernetes v1.17)

## Start

- `docker-compose up`

## View

- Go to `http://localhost:5000/` to watch events and browse state.

## Test

- `python3 -m unittest tests/unit.py`

## View

- Go to `http://localhost:8001/` to see what's in Redis (use `redis:6379`)

## Stop

- `docker-compose down`
