package txn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected Transaction
		hasError bool
	}{
		{
			name: "Valid transaction with null value",
			data: []byte(`["r", 1, null]`),
			expected: Transaction{
				Op:  "r",
				Key: 1,
				Val: nil,
			},
			hasError: false,
		},
		{
			name: "Valid transaction with value",
			data: []byte(`["w", 2, 9]`),
			expected: Transaction{
				Op:  "w",
				Key: 2,
				Val: func() *int64 { i := int64(9); return &i }(),
			},
			hasError: false,
		},
		{
			name:     "Invalid transaction: not enough elements",
			data:     []byte(`["r", 1]`),
			hasError: true,
		},
		{
			name:     "Invalid transaction: too many elements",
			data:     []byte(`["r", 1, 2, 3]`),
			hasError: true,
		},
		{
			name:     "Invalid transaction: invalid types",
			data:     []byte(`[1, "r", null]`),
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var txn Transaction
			err := txn.UnmarshalJSON(tt.data)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, txn)
			}
		})
	}
}

func TestTransactionMarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		txn      Transaction
		expected []byte
		hasError bool
	}{
		{
			name: "Valid transaction with null value",
			txn: Transaction{
				Op:  "r",
				Key: 1,
				Val: nil,
			},
			expected: []byte(`["r",1,null]`),
			hasError: false,
		},
		{
			name: "Valid transaction with value",
			txn: Transaction{
				Op:  "w",
				Key: 2,
				Val: func() *int64 { i := int64(9); return &i }(),
			},
			expected: []byte(`["w",2,9]`),
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.txn.MarshalJSON()
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, data)
			}
		})
	}
}
