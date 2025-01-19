package txn

import (
	"encoding/json"
	"fmt"
)

type Transaction struct {
	Op  string `json:"op"`
	Key int64  `json:"key"`
	Val *int64 `json:"val"`
}

func (t *Transaction) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if len(raw) != 3 {
		return fmt.Errorf("invalid transaction data: expected 3 elements, got %d", len(raw))
	}

	if err := json.Unmarshal(raw[0], &t.Op); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &t.Key); err != nil {
		return err
	}
	if len(raw[2]) > 0 && string(raw[2]) != "null" {
		var val int64
		if err := json.Unmarshal(raw[2], &val); err != nil {
			return err
		}
		t.Val = &val
	}

	return nil
}

func (t Transaction) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{t.Op, t.Key, t.Val})
}
