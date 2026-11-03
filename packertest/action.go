package packertest

import (
	"fmt"
	"math/rand/v2"
)

type FuzzAction struct {
	Name   string
	Weight int
	Run    func() error
}

func PickAndRunAction(actions []FuzzAction, rng *rand.Rand) error {
	totalWeigth := 0
	for _, a := range actions {
		totalWeigth += a.Weight
	}
	if totalWeigth <= 0 {
		return nil
	}

	target := rng.IntN(totalWeigth)

	for _, a := range actions {
		if target < a.Weight {
			if err := a.Run(); err != nil {
				return fmt.Errorf("running action '%s':\n%w", a.Name, err)
			}
			return nil
		}
		target -= a.Weight
	}

	panic("did not run any action")
}
