.check-input:
	@if [ -z "$(CHALLENGE)" ]; then \
		echo "CHALLENGE variable is not set"; \
		exit 1; \
	fi

.go-build: .check-input
	@echo "Building go binary..."
	@mkdir -p bin
	@go build -o ./bin/$(CHALLENGE) ./$(CHALLENGE)

run: .go-build
	@echo "Running $(CHALLENGE)..."
	maelstrom test -w $(CHALLENGE) --bin ./bin/$(CHALLENGE) --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
