package broadcast

func (p *Program) read(body workload) map[string]any {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	resp := map[string]any{
		"type":     "read_ok",
		"messages": p.messages,
	}
	return resp
}
