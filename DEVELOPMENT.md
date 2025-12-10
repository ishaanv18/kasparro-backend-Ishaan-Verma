# Development Notes

My personal notes while building this project. First time doing something this complex!

## Week 1 - Getting Started

### Day 1 - Project Setup
- Watched some FastAPI tutorials on YouTube first
- Started with Flask because that's what we learned in class, but professor suggested FastAPI
- Docker setup was HARD on Windows - had to install WSL2 and everything
- Spent 2 hours just getting PostgreSQL to connect properly lol

### Day 2 - Understanding ETL
- Had to Google what ETL actually means in production (Extract, Transform, Load)
- CoinPaprika API docs are confusing - tried multiple endpoints before finding the right one
- Learned about async/await in Python (we only did basic Python in college)
- Got first API call working - felt amazing!

### Day 3 - Building the Pipeline
- Checkpoint system took forever to understand
- Kept getting duplicate data until I figured out ON CONFLICT
- CSV processing is easy compared to APIs
- Added Pydantic after getting too many bugs - should have done this earlier!

### Day 4-5 - API and Testing
- FastAPI auto-generates docs which is SO cool
- Wrote tests (first time using pytest properly)
- Docker compose finally makes sense now
- Still not sure if my error handling is good enough

### Day 6-7 - Extra Features
- Added Prometheus metrics (learned about this from a Medium article)
- Schema drift detection seemed impressive so tried implementing it
- GitHub Actions for CI/CD (copied from a tutorial but modified it)
- Tried to make everything look professional

## Challenges Faced

### API Rate Limiting
Initially hit rate limits constantly. Solution:
- Added configurable rate limiting
- Implemented basic backoff (could be better)
- TODO: Add exponential backoff

### Schema Inconsistencies
Different APIs return data in different formats:
- CoinPaprika uses nested "quotes" object
- CoinGecko is more straightforward
- Had to write custom normalization for each

### Docker on Windows
- WSL2 setup was painful
- Had to adjust file paths for Windows compatibility
- Make doesn't work natively (added docker-compose commands)

## Future Improvements

- [ ] Add caching layer (Redis?)
- [ ] Better error recovery
- [ ] Add data validation alerts
- [ ] Implement data quality checks
- [ ] Add more comprehensive logging

## Useful Commands

```bash
# Quick restart after code changes
docker compose restart api

# Check what's happening
docker compose logs -f etl

# Clean slate
docker compose down -v && docker compose up -d --build
```

## Resources Used

- FastAPI docs: https://fastapi.tiangolo.com/
- SQLAlchemy async: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
- Prometheus Python client: https://github.com/prometheus/client_python
- Stack Overflow (obviously)

## Notes to Self

- Remember to update .env with real API keys before deployment
- Test with actual API keys, not just mocks
- The ETL runs every 30 minutes - adjust if needed
- Database migrations need to be run manually for now

---

Last updated: December 2025
