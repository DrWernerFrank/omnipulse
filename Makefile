COMPOSE ?= docker compose

.PHONY: up down logs ps test backend-shell mcp open seed verify

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

# Full agent stack: backend + frontend + stdio MCP server (docker attach omnipulse-mcp)
mcp:
	$(COMPOSE) --profile agents up -d --build

backend-shell:
	$(COMPOSE) exec backend bash

open:
	start http://localhost:3000

# Unit tests run against the local Python source tree
test:
	cd backend && python -m pytest tests -q

# Static syntax verification without any external dependencies
verify:
	python -m compileall -q backend/app
