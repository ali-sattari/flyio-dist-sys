CHALLENGE ?=
SUB ?=
BIN := ./bin/app
BASE_CMD := maelstrom test -w $(CHALLENGE) --bin $(BIN)

.build:
	@echo "Building the project..."
	@mkdir -p bin
	@go build -C ./$(CHALLENGE)/ -o ../$(BIN)

run: .build
# check if CHALLENGE is empty
ifeq ($(CHALLENGE),)
	$(error "CHALLENGE is required. Usage: make run CHALLENGE=broadcast SUB=3a")
endif
	@echo "Running with $(CHALLENGE) / $(SUB)"

## echo
ifeq ($(CHALLENGE),echo)
	$(BASE_CMD) --node-count 1 --time-limit 10;
endif

## unique-ids
ifeq ($(CHALLENGE),unique-ids)
	$(BASE_CMD) --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition;
endif

## broadcast
ifeq ($(CHALLENGE),broadcast)
ifeq ($(SUB),)
   	$(error "SUB is required. Usage: make run CHALLENGE=broadcast SUB=3a")
endif
## broadcast -> 3a: single-node
ifeq ($(SUB),3a)
	$(BASE_CMD) --node-count 1 --time-limit 20 --rate 10;
## broadcast -> 3b: multi-node
else ifeq ($(SUB),3b)
	$(BASE_CMD) --node-count 5 --time-limit 20 --rate 10;
## broadcast -> 3c: fault tolerant
else ifeq ($(SUB),3c)
	$(BASE_CMD) --node-count 5 --time-limit 20 --rate 10 --nemesis partition;
## broadcast -> 3d: efficient part 1 & 2
else ifeq ($(SUB),3d)
	$(BASE_CMD)	--node-count 25 --time-limit 20 --rate 100 --latency 100;
else
	@echo "Unknown sub-challenge for $(CHALLENGE): $(SUB)"
endif
endif
## grow only counter
ifeq ($(CHALLENGE),g-counter)
	$(BASE_CMD) --node-count 3 --rate 100 --time-limit 20 --nemesis partition;
endif
## kafka log
ifeq ($(CHALLENGE),kafka)
ifeq ($(SUB),)
	$(error "SUB is required. Usage: make run CHALLENGE=kafkla SUB=5a")
endif
## kafka log -> 5a: single-node
ifeq ($(SUB),5a)
	$(BASE_CMD) --node-count 1 --concurrency 2n --time-limit 20 --rate 1000;
else ifeq ($(SUB),5b)
	$(BASE_CMD) --node-count 2 --concurrency 2n --time-limit 20 --rate 1000;
else ifeq ($(SUB),5c)
	$(BASE_CMD) --node-count 2 --concurrency 2n --time-limit 20 --rate 1000;
else
	@echo "Unknown sub-challenge for $(CHALLENGE): $(SUB)"
endif
## txn-rw-register log
ifeq ($(CHALLENGE),txn-rw-register)
ifeq ($(SUB),)
	$(error "SUB is required. Usage: make run CHALLENGE=txn-rw-register SUB=6a")
endif
## txn-rw-register log -> 6a: single-node
ifeq ($(SUB),56a)
	$(BASE_CMD) --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total;
else
	@echo "Unknown sub-challenge for $(CHALLENGE): $(SUB)"
endif

else
	@echo "Unknown challenge: $(CHALLENGE)"
endif
