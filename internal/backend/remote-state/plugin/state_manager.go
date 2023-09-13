package plugin

import (
	"context"

	"github.com/marcinwyszynski/backendplugin"

	"github.com/placeholderplaceholderplaceholder/opentf/internal/states/remote"
	"github.com/placeholderplaceholderplaceholder/opentf/internal/states/statemgr"
)

type stateManager struct {
	workspace string

	client backendplugin.BackendPlugin
}

func (s *stateManager) Get() (*remote.Payload, error) {
	payload, err := s.client.GetStatePayload(context.Background(), s.workspace)
	if err != nil {
		return nil, err
	} else if payload == nil {
		return nil, nil
	}

	return &remote.Payload{
		MD5:  payload.MD5,
		Data: payload.Data,
	}, nil
}

func (s *stateManager) Put(data []byte) error {
	return s.client.PutState(context.Background(), s.workspace, data)
}

func (s *stateManager) Delete() error {
	return s.client.DeleteState(context.Background(), s.workspace)
}

func (s *stateManager) Lock(info *statemgr.LockInfo) (string, error) {
	return s.client.LockState(context.Background(), s.workspace, &backendplugin.LockInfo{
		ID:        info.ID,
		Operation: info.Operation,
		Info:      info.Info,
		Who:       info.Who,
		Version:   info.Version,
		Created:   info.Created,
		Path:      info.Path,
	})
}

func (s *stateManager) Unlock(id string) error {
	return s.client.UnlockState(context.Background(), s.workspace, id)
}
