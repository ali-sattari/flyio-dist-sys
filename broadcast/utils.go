package broadcast

func contains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func remove[T any](slice []T, i int) []T {
	if i > len(slice) {
		return slice
	}
	if i == len(slice) {
		return slice[:len(slice)-1]
	}
	slice[i] = slice[len(slice)-1]
	return slice[:len(slice)-1]
}
