.PHONY: fmt lint check

fmt:
	poetry run isort . && poetry run black .

lint:
	poetry run flake8 .

check:
	poetry run isort --check . && poetry run black --check .

.PHONY: investigate

investigate:
	poetry install && \
	poetry run python crm_messaging_investigation/raw_exploratory_bases/exploratory_campaigns.py && \
	poetry run python crm_messaging_investigation/raw_exploratory_bases/exploratory_conversation.py && \
	poetry run python crm_messaging_investigation/raw_exploratory_bases/exploratory_logs.py && \
	poetry run python crm_messaging_investigation/investigation_campaigns/campaign_apple.py && \
	poetry run python crm_messaging_investigation/investigation_campaigns/campaign_samsung.py