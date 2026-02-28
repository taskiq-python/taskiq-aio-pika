.DEFAULT:
	@echo "No such command (or you pass two or many targets to ). List of possible commands: make help"

.DEFAULT_GOAL := help

##@ Local development

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target> <arg=value>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m  %s\033[0m\n\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: clear_rabbit
clear_rabbit:  ## Clear RabbitMQ data volume and restart container
	@docker stop taskiq_aio_pika_rabbitmq && docker rm taskiq_aio_pika_rabbitmq &&  docker volume rm taskiq-aio-pika_rabbitmq_data && docker compose up -d
