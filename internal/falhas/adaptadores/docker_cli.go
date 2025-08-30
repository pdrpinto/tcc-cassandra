package adaptadores

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type OrquestradorDeFalhasDockerCLI struct{}

func NovoOrquestradorDeFalhasDockerCLI() *OrquestradorDeFalhasDockerCLI {
	return &OrquestradorDeFalhasDockerCLI{}
}

func (o *OrquestradorDeFalhasDockerCLI) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %v: %w - %s", args, err, string(out))
	}
	return nil
}

func (o *OrquestradorDeFalhasDockerCLI) PausarNo(ctx context.Context, nomeDoContainer string) error {
	return o.run(ctx, "pause", nomeDoContainer)
}

func (o *OrquestradorDeFalhasDockerCLI) ContinuarNo(ctx context.Context, nomeDoContainer string) error {
	return o.run(ctx, "unpause", nomeDoContainer)
}

func (o *OrquestradorDeFalhasDockerCLI) PararNo(ctx context.Context, nomeDoContainer string, timeoutSegundos int) error {
	if timeoutSegundos <= 0 {
		timeoutSegundos = 10
	}
	return o.run(ctx, "stop", "-t", fmt.Sprintf("%d", timeoutSegundos), nomeDoContainer)
}

func (o *OrquestradorDeFalhasDockerCLI) DesconectarNoDaRede(ctx context.Context, nomeDoContainer string, nomeDaRede string, forcar bool) error {
	if nomeDaRede == "" {
		return errors.New("nome da rede obrigatorio")
	}
	if forcar {
		return o.run(ctx, "network", "disconnect", "-f", nomeDaRede, nomeDoContainer)
	}
	return o.run(ctx, "network", "disconnect", nomeDaRede, nomeDoContainer)
}

func (o *OrquestradorDeFalhasDockerCLI) ReconectarNoARede(ctx context.Context, nomeDoContainer string, nomeDaRede string) error {
	if nomeDaRede == "" {
		return errors.New("nome da rede obrigatorio")
	}
	// aguarda rede aparecer (robustez)
	prazo, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var ultimaErr error
	for prazo.Err() == nil {
		if err := o.run(ctx, "network", "connect", nomeDaRede, nomeDoContainer); err == nil {
			return nil
		} else {
			ultimaErr = err
			time.Sleep(500 * time.Millisecond)
		}
	}
	return ultimaErr
}
