

fmt:
    black .
    isort .

bump_patch:
    #! /usr/bin/env fish
    set crt_version (poetry version | sed -E 's/.*([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
    bump2version patch --current-version $crt_version pyproject.toml --allow-dirty

bump_minor:
    #! /usr/bin/env fish
    set crt_version (poetry version | sed -E 's/.*([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
    bump2version minor --current-version $crt_version pyproject.toml --allow-dirty

