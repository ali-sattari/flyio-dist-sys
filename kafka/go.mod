module main

go 1.23.3

require (
	github.com/jepsen-io/maelstrom/demo/go v0.0.0-20241204184542-016086a90274
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace snowflaker => ../unique-ids/pkg/snowflaker
