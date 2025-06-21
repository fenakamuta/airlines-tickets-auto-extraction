# Airlines Tickets Auto Extraction

Sistema automatizado para extração de dados de passagens aéreas da ANAC (Agência Nacional de Aviação Civil) e LATAM Airlines, com armazenamento em Google Cloud Storage e BigQuery.

## 📋 Descrição

Este projeto automatiza a coleta de dados de passagens aéreas de duas fontes principais:

1. **ANAC (Agência Nacional de Aviação Civil)**: Extrai dados contábeis e financeiros das empresas aéreas regulamentadas
2. **LATAM Airlines**: Coleta informações de voos disponíveis entre aeroportos específicos

Os dados extraídos são armazenados no Google Cloud Storage e podem ser carregados no BigQuery para análise.

## 🚀 Funcionalidades

- **Extração ANAC**: Download automático de arquivos ZIP do site da ANAC, extração e upload para GCS
- **Extração LATAM**: Web scraping da LATAM Airlines para coletar dados de voos
- **Armazenamento**: Upload automático para Google Cloud Storage
- **BigQuery**: Carregamento de dados para análise no BigQuery
- **Automação**: Processamento em lote com suporte a intervalos de datas

## 📦 Pré-requisitos

- Python 3.8+
- Google Cloud Platform account
- Credenciais de serviço do Google Cloud
- Navegador Chrome (para Playwright)

## 🔧 Instalação

### 1. Clone o repositório

```bash
git clone <repository-url>
cd airlines-tickets-auto-extraction
```

### 2. Crie um ambiente virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Configure o Playwright

```bash
playwright install chromium
```

### 5. Configure as credenciais do Google Cloud

1. Baixe o arquivo de credenciais JSON do Google Cloud Console
2. Coloque o arquivo no diretório raiz do projeto
3. Configure a variável de ambiente:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
```

### 6. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
# Google Cloud Storage
GOOGLE_GCS_BUCKET_NAME=your-bucket-name
GCS_CREDENTIALS_PATH=path/to/your/credentials.json

# ANAC Configuration
ANAC_YEAR=2024
TEMP_DIR=./anac_temp
LOG_LEVEL=INFO

# LATAM Configuration
LATAM_FLIGHT_ORIGIN_NAME=São Paulo
LATAM_FLIGHT_DESTINATION_NAME=Rio de Janeiro
LATAM_FLIGHT_ORIGIN_AIRPORT=GRU
LATAM_FLIGHT_DESTINATION_AIRPORT=GIG
LATAM_FLIGHT_DATE_START_=2025-06-30
LATAM_FLIGHT_DATE_END_=2026-01-01
```

## 🎯 Como Usar

### Extração de dados da ANAC

```bash
cd src
python -m anac.aviation
```

### Extração de dados da LATAM

```bash
cd src
python extract_latam.py
```

### Carregamento para BigQuery

```bash
# Para dados da ANAC
cd src
python -m anac.load_to_bigquery

# Para dados da LATAM
cd src
python -m latam.load_to_bigquery
```

## 📁 Estrutura do Projeto

```
airlines-tickets-auto-extraction/
├── src/
│   ├── anac/
│   │   ├── aviation.py          # Extração principal da ANAC
│   │   └── load_to_bigquery.py  # Carregamento ANAC para BigQuery
│   ├── latam/
│   │   ├── flights.py           # Extração de voos LATAM
│   │   ├── utils.py             # Utilitários LATAM
│   │   └── load_to_bigquery.py  # Carregamento LATAM para BigQuery
│   ├── config.py                # Configurações do projeto
│   ├── extract_anac.py          # Script de extração ANAC
│   ├── extract_latam.py         # Script de extração LATAM
│   ├── load_bigquery_anac.py    # Script de carregamento ANAC
│   └── load_bigquery_latam.py   # Script de carregamento LATAM
├── requirements.txt             # Dependências Python
├── .env                        # Variáveis de ambiente (criar)
└── README.md                   # Este arquivo
```

## 🔄 Reproducibilidade

### Configuração do Ambiente

Para garantir a reprodutibilidade, siga estes passos:

1. **Versão do Python**: Use Python 3.8 ou superior
2. **Dependências**: Todas as dependências estão listadas em `requirements.txt`
3. **Ambiente Virtual**: Sempre use um ambiente virtual isolado
4. **Credenciais**: Configure as credenciais do Google Cloud corretamente

### Scripts de Automação

#### Para ANAC:

```bash
#!/bin/bash
# Script para extração ANAC completa
cd src
python -m anac.aviation
python -m anac.load_to_bigquery
```

#### Para LATAM:

```bash
#!/bin/bash
# Script para extração LATAM completa
cd src
python extract_latam.py
python -m latam.load_to_bigquery
```

### Verificação de Dados

Após a extração, verifique se os dados foram carregados corretamente:

1. **Google Cloud Storage**: Verifique o bucket configurado
2. **BigQuery**: Execute queries para validar os dados carregados

## 🛠️ Configurações Avançadas

### Logs

O sistema utiliza logging configurável. Para alterar o nível de log:

```env
LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

### Diretório Temporário

Para ANAC, você pode configurar um diretório temporário personalizado:

```env
TEMP_DIR=/path/to/temp/directory
```

### Intervalos de Data (LATAM)

Configure os intervalos de data para extração da LATAM:

```env
LATAM_FLIGHT_DATE_START_=2025-01-01
LATAM_FLIGHT_DATE_END_=2025-12-31
```

## 🐛 Solução de Problemas

### Problemas Comuns

1. **Erro de credenciais do Google Cloud**
   - Verifique se o arquivo de credenciais está correto
   - Confirme se as permissões estão configuradas adequadamente

2. **Erro do Playwright**
   - Execute `playwright install chromium`
   - Verifique se o Chrome está instalado

3. **Timeout na extração**
   - Aumente os timeouts no código se necessário
   - Verifique a conectividade com a internet

4. **Arquivos não encontrados (ANAC)**
   - Verifique se o ano configurado está correto
   - Confirme se o site da ANAC está acessível

### Logs de Debug

Para debug detalhado, configure:

```env
LOG_LEVEL=DEBUG
```

## 📊 Estrutura dos Dados

### Dados ANAC
- Arquivos contábeis das empresas aéreas
- Armazenados em: `anac_data/` no GCS
- Formato: Arquivos de texto extraídos dos ZIPs

### Dados LATAM
- Informações de voos disponíveis
- Armazenados em: `latam/YYYY-MM-DD/` no GCS
- Formato: CSV com dados de voos

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença especificada no arquivo `LICENSE`.
---

**Nota**: Este projeto é destinado para uso educacional e de pesquisa. Respeite os termos de uso dos sites acessados e as políticas de privacidade aplicáveis.