CHALLENGE ?=
SUB ?=
BIN := ./bin/app
BASE_CMD := maelstrom test -w $(CHALLENGE) --bin $(BIN)

.build:
	@echo "Building the project..."
	@mkdir -p bin
	@go build -o $(BIN) .

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
## broadcast -> 3a
ifeq ($(SUB),3a)
	$(BASE_CMD) --node-count 1 --time-limit 20 --rate 10;
## broadcast -> 3b
else ifeq ($(SUB),3b)
	$(BASE_CMD) --node-count 5 --time-limit 20 --rate 10;
## broadcast -> 3c
else ifeq ($(SUB),3c)
	$(BASE_CMD) --node-count 5 --time-limit 20 --rate 10 --nemesis partition;
else
	@echo "Unknown sub-challenge for $(CHALLENGE): $(SUB)"
endif
else
	@echo "Unknown challenge: $(CHALLENGE)"
endif
