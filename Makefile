.PHONY: fmt lint

fmt:
	go tool goimports -w choreography/ orchestration/

lint:
	@for dir in choreography/*/; do \
		echo "=> vet $$dir"; \
		(cd "$$dir" && go vet ./...); \
	done
	@for dir in orchestration/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "=> vet $$dir"; \
			(cd "$$dir" && go vet ./...); \
		fi; \
	done
