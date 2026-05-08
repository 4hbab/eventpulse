.PHONY: rabbit run test clean

rabbit:
	docker compose up -d

rabbit-down:
	docker compose down

run:
	go run ./cmd/eventpulse

test:
	go test ./... -v

tidy:
	go mod tidy
	go fmt ./...
	go vet ./...