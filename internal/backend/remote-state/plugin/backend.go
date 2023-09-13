package plugin

import (
	"context"
	"fmt"
	"os/exec"

	goplugin "github.com/hashicorp/go-plugin"
	"github.com/marcinwyszynski/backendplugin"

	"github.com/placeholderplaceholderplaceholder/opentf/internal/backend"
	"github.com/placeholderplaceholderplaceholder/opentf/internal/legacy/helper/schema"
	"github.com/placeholderplaceholderplaceholder/opentf/internal/logging"
	"github.com/placeholderplaceholderplaceholder/opentf/internal/states"
	"github.com/placeholderplaceholderplaceholder/opentf/internal/states/remote"
	"github.com/placeholderplaceholderplaceholder/opentf/internal/states/statemgr"
)

type Backend struct {
	*schema.Backend

	// The fields below are set from configure.
	client backendplugin.BackendPlugin
}

func New() backend.Backend {
	s := &schema.Backend{
		Schema: map[string]*schema.Schema{
			// For the sake of simplicity, let's start with the source being
			// the path to the binary that needs to be executed.
			//
			// In reality, proper plugin discovery needs to be implemented.
			"source": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Source of the plugin",
			},

			// Ignore for now, until plugin discovery is implemented.
			"version": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Version of the plugin. If not supplied, the latest is used",
			},

			// The configuration for the plugin is passed as a map of strings
			// to strings. This is a bit of a hack, but it's the only way to
			// pass arbitrary configuration to a backend plugin without reworking
			// the entire backend plugin system.
			"config": {
				Type:        schema.TypeMap,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Required:    true,
				Description: "Configuration for the plugin as a string-to-string map",
			},
		},
	}

	result := &Backend{Backend: s}
	result.Backend.ConfigureFunc = result.configure
	return result
}

func (b *Backend) StateMgr(workspace string) (statemgr.Full, error) {
	workspaces, err := b.workspaces()
	if err != nil {
		return nil, fmt.Errorf("failed to list workspaces: %w", err)
	}

	var found bool
	for _, w := range workspaces {
		if w == workspace {
			found = true
			break
		}
	}

	// We need to create the object so it's listed by States.
	if !found {
		if err := b.createWorkspace(workspace); err != nil {
			return nil, err
		}
	}

	return b.stateMgr(workspace), nil
}

func (b *Backend) DeleteWorkspace(workspace string, force bool) error {
	return b.client.DeleteWorkspace(context.Background(), workspace, force)
}

func (b *Backend) Workspaces() ([]string, error) {
	workspaces, err := b.client.ListWorkspaces(context.Background())
	if err != nil {
		return nil, err
	}

	var reportedDefault bool

	for _, w := range workspaces {
		if w == backend.DefaultStateName {
			reportedDefault = true
			break
		}
	}

	if !reportedDefault {
		workspaces = append(workspaces, backend.DefaultStateName)
	}

	return workspaces, nil
}

func (b *Backend) createWorkspace(workspace string) error {
	stateManager := b.stateMgr(workspace)

	// take a lock on this state while we write it
	lockInfo := statemgr.NewLockInfo()
	lockInfo.Operation = "init"
	lockId, err := b.client.LockState(context.Background(), workspace, &backendplugin.LockInfo{
		ID:        lockInfo.ID,
		Operation: lockInfo.Operation,
		Info:      lockInfo.Info,
		Who:       lockInfo.Who,
		Version:   lockInfo.Version,
		Created:   lockInfo.Created,
		Path:      lockInfo.Path,
	})
	if err != nil {
		return fmt.Errorf("failed to lock plugin state: %s", err)
	}

	// Local helper function so we can call it multiple places
	lockUnlock := func(parent error) error {
		if err := stateManager.Unlock(lockId); err != nil {
			return err
		}
		return parent
	}

	// Grab the value
	// This is to ensure that no one beat us to writing a state between
	// the `exists` check and taking the lock.
	if err := stateManager.RefreshState(); err != nil {
		err = lockUnlock(err)
		return err
	}

	// If we have no state, we have to create an empty state
	if v := stateManager.State(); v == nil {
		if err := stateManager.WriteState(states.NewState()); err != nil {
			err = lockUnlock(err)
			return err
		}
		if err := stateManager.PersistState(nil); err != nil {
			err = lockUnlock(err)
			return err
		}
	}

	// Unlock, the state should now be initialized
	return lockUnlock(nil)
}

// This is where the magic needs to happen.
func (b *Backend) configure(ctx context.Context) error {
	config := schema.FromContextBackendConfig(ctx)

	source := config.Get("source").(string)

	client := goplugin.NewClient(&goplugin.ClientConfig{
		Logger:           logging.NewLogger("backend"),
		HandshakeConfig:  backendplugin.Handshake,
		Plugins:          backendplugin.Plugins,
		Cmd:              exec.Command("sh", "-c", source),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
	})

	rpcClient, err := client.Client()
	if err != nil {
		return fmt.Errorf("failed to create plugin gRPC client: %w", err)
	}

	// Request the plugin
	raw, err := rpcClient.Dispense(backendplugin.BackendPluginName)
	if err != nil {
		return fmt.Errorf("failed to dispense the backend plugin: %w", err)
	}

	b.client = raw.(backendplugin.BackendPlugin)

	rawConfig := config.Get("config").(map[string]any)
	strConfig := make(map[string]string, len(rawConfig))

	for k, v := range rawConfig {
		strConfig[k] = v.(string)
	}

	// TODO: kill the client when we're done with it. We will need to teach the
	// rest of the code that the backend implements io.Closer.
	return b.client.Configure(ctx, strConfig)
}

func (b *Backend) workspaces() ([]string, error) {
	return b.client.ListWorkspaces(context.Background())
}

func (b *Backend) stateMgr(workspace string) statemgr.Full {
	return &remote.State{
		Client: &stateManager{
			workspace: workspace,
			client:    b.client,
		},
	}
}
