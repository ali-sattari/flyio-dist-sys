CHALLENGES = echo unique-ids broadcast g-counter kafka txn

.go-build: 
	@echo "Building $(CHALLENGE)..."
	@mkdir -p bin
	@go build -o ./bin/$(CHALLENGE) ./$(CHALLENGE)


run:
	@echo "Run using run-X where x is challenge name: $(CHALLENGES)"

run-%:
	@make .go-build CHALLENGE=${@:run-%=%}
	@make .challenge CHALLENGE=${@:run-%=%}

.challenge:
	@echo "Running $(CHALLENGE)..."

	@if [ "$(CHALLENGE)" = "echo" ]; then \
		maelstrom test -w $(CHALLENGE) --bin ./bin/$(CHALLENGE) --node-count 1 --time-limit 10; \
	elif [ "$(CHALLENGE)" = "unique-ids" ]; then \
		maelstrom test -w $(CHALLENGE) --bin ./bin/$(CHALLENGE) --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition; \
	elif [ "$(CHALLENGE)" = "broadcast" ]; then \
		maelstrom test -w $(CHALLENGE) --bin ./bin/$(CHALLENGE) --node-count 1 --time-limit 20 --rate 10; \
	elif [ "$(CHALLENGE)" = "g-counter" ]; then \
		maelstrom test -w $(CHALLENGE) --bin ./bin/$(CHALLENGE) --node-count 3 --rate 100 --time-limit 20 --nemesis partition; \
	elif [ "$(CHALLENGE)" = "kafka" ]; then \
		maelstrom test -w $(CHALLENGE) --bin ./bin/$(CHALLENGE) --node-count 1 --rate 1000 --time-limit 20 --concurrency 2n; \
	elif [ "$(CHALLENGE)" = "txn" ]; then \
		maelstrom test -w txn-rw-register --bin ./bin/$(CHALLENGE) --node-count 1 --rate 1000 --time-limit 20 --concurrency 2n --consistency-models read-uncommitted --availability total; \
	fi

.PHONY: run run-%