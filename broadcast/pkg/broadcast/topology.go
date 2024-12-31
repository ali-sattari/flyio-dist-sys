package broadcast

import "log"

func (p *Program) topology(body workload) map[string]any {
	p.topo = body.Topology
	resp := map[string]any{
		"type": "topology_ok",
	}
	p.addLongRangeLinks()
	return resp
}

func (p *Program) addLongRangeLinks() {
	nodes := p.node.NodeIDs()
	numLinks := int(len(nodes) / 3)

	// Add long-range links
	nc := len(nodes)
	for i := 0; i < numLinks; i++ {
		for {
			// Select two distinct nodes from left and right
			l, r := nodes[i], nodes[nc-i-1]
			if l != r && !contains(p.topo[l], r) {
				// Add bidirectional link
				p.topo[l] = append(p.topo[l], r)
				p.topo[r] = append(p.topo[r], l)
				break
			}
		}
	}
	log.Printf("added long range links to topology: %+v", p.topo)
}
