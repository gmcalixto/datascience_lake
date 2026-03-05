# datascience_lake

## Descrição

Este projeto implementa um data lake simples para demonstração de conceitos de data science, utilizando MongoDB como armazenamento e Python para processamento de dados. Inclui scripts para geração de dados JSON, ingestão em MongoDB e criação de visualizações.

## Estrutura do Projeto

- `docker-compose.yml`: Configuração do ambiente Docker, incluindo MongoDB.
- `lake/`: Scripts Python principais.
  - `generate_json_dataset.py`: Gera conjuntos de dados JSON.
  - `ingest_jsonl.py`: Ingesta dados JSONL no MongoDB.
  - `init.py`: Inicialização do ambiente.
  - `plots_from_mongo.py`: Gera plots a partir dos dados no MongoDB.
- `lake_data/json/`: Dados de exemplo.
  - `events_10000.jsonl`: Arquivo JSONL com 10.000 eventos.
  - `sample_20/`: Amostras individuais de dados JSON.
- `out_lake_plots/`: Diretório para saída de plots.

## Instalação

1. Certifique-se de ter Docker e Docker Compose instalados.
2. Clone o repositório.
3. Execute `docker-compose up` para iniciar o MongoDB.
4. Instale as dependências Python necessárias (verifique os imports nos scripts, como `pymongo`, `matplotlib`).

## Uso

1. Execute os scripts Python na ordem apropriada: `init.py`, `generate_json_dataset.py`, `ingest_jsonl.py`, `plots_from_mongo.py`.
2. Verifique os plots gerados em `out_lake_plots/`.

## Contribuição

Contribuições são bem-vindas. Abra issues ou pull requests no repositório.