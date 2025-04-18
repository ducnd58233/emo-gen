# Setup Project

- Create environment (if not already created)
```
conda create -n emogen-crawler python=3.12
conda activate emogen-crawler
pip install setuptools
pip install poetry
```

- Activate conda environment
```
conda activate emogen-crawler
```

- Install dependencies
```
poetry install
```

- Install pre-commit hooks
```
poetry run pre-commit install
```

- Run pre-commit hooks
```
poetry run pre-commit run --all-files
```

- Run if poetry has issues
```
pip install --upgrade poetry
```

- Run docker compose
```
docker compose -f deployments/docker/docker-compose.<env>.yml up -d
```
`<env>` can be `local`, `dev`, `prod`

- Create `.env` file
```
cp .env.example .env
```

- Run crawler
```
poetry run python src/main.py
```

- Run crawler with debugger
```
poetry run python -m pdb src/main.py
```
