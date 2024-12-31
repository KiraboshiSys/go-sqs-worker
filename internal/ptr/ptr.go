package ptr

func Of[T any](val T) *T {
	return &val
}

func Unwrap[T any](ptr *T) T {
	if ptr == nil {
		return *new(T)
	}
	return *ptr
}
