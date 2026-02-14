# Guia Rápido de Uso 🚀

## Instalação em 3 Passos

### 1. Clone o Repositório
```bash
git clone https://github.com/yourusername/weather-analytics-dashboard.git
cd weather-analytics-dashboard
```

### 2. Execute o Setup Automático
```bash
chmod +x setup.sh
./setup.sh
```

### 3. Inicie o Dashboard
```bash
source venv/bin/activate
streamlit run src/app.py
```

Pronto! O dashboard abrirá em `http://localhost:8501` 🎉

---

## Uso Avançado

### Executar Testes
```bash
# Todos os testes
pytest tests/ -v

# Com relatório de cobertura
pytest tests/ --cov=src --cov-report=html

# Abrir relatório HTML
open htmlcov/index.html
```

### Configurar Automação com Cron

1. Torne o script executável:
```bash
chmod +x run_pipeline.sh
```

2. Adicione ao crontab:
```bash
crontab -e
```

3. Adicione a linha (executa a cada 6 horas):
```
0 */6 * * * /caminho/completo/para/weather-analytics-dashboard/run_pipeline.sh
```

### Usar com Airflow

1. Instale o Airflow:
```bash
pip install apache-airflow
```

2. Inicialize o banco:
```bash
airflow db init
```

3. Copie a DAG:
```bash
cp dags/weather_pipeline_dag.py ~/airflow/dags/
```

4. Inicie os serviços:
```bash
# Terminal 1
airflow webserver -p 8080

# Terminal 2
airflow scheduler
```

5. Acesse: `http://localhost:8080`

---

## Comandos Úteis

### Desenvolvimento
```bash
# Formatar código
black src tests

# Verificar estilo
flake8 src tests

# Verificar tipos
mypy src
```

### Análise de Dados
```bash
# Coletar dados manualmente
python run_pipeline.sh

# Ver últimos dados coletados
ls -lht data/
```

### Dashboard
```bash
# Rodar em modo desenvolvedor com auto-reload
streamlit run src/app.py --server.runOnSave true

# Rodar em porta customizada
streamlit run src/app.py --server.port 8502

# Limpar cache e recarregar
# Use o botão "Refresh Data" no dashboard
```

---

## Estrutura de Dados

### CSV de Saída
Os arquivos CSV gerados contêm:

**weather_data_TIMESTAMP.csv**
- `city`: Nome da cidade
- `time`: Data e hora
- `temperature_2m`: Temperatura (°C)
- `relative_humidity_2m`: Umidade relativa (%)
- `precipitation`: Precipitação (mm)
- `wind_speed_10m`: Velocidade do vento (km/h)
- `cloud_cover`: Cobertura de nuvens (%)
- `temp_ma`: Média móvel da temperatura
- `temp_anomaly`: Flag de anomalia (True/False)

**alerts_TIMESTAMP.csv**
- `type`: Tipo de alerta
- `city`: Cidade afetada
- `time`: Hora do alerta
- `value`: Valor que gerou o alerta
- `message`: Mensagem descritiva

---

## Solução de Problemas

### Erro: "Module not found"
```bash
# Reinstale as dependências
pip install -r requirements.txt --force-reinstall
```

### Erro: "Port already in use"
```bash
# Use outra porta
streamlit run src/app.py --server.port 8502
```

### Erro: "API request failed"
```bash
# Verifique sua conexão com internet
# A API Open-Meteo pode estar temporariamente indisponível
# O sistema tentará novamente automaticamente
```

### Dashboard não carrega dados
```bash
# Limpe o cache do Streamlit
# Pressione 'c' no terminal ou use o botão "Refresh Data"
```

---

## Personalização

### Adicionar Novas Cidades

Edite `config/config.yaml`:
```yaml
cities:
  - name: "Nova Cidade"
    lat: -23.5505
    lon: -46.6333
```

### Ajustar Alertas

Edite `config/config.yaml`:
```yaml
alerts:
  temperature:
    high_threshold: 38  # Novo limite
    low_threshold: 3    # Novo limite
```

### Modificar Frequência de Atualização

**Para Cron:**
```bash
# A cada 3 horas
0 */3 * * * /caminho/para/run_pipeline.sh

# Diariamente às 8h
0 8 * * * /caminho/para/run_pipeline.sh
```

**Para Airflow:**
Edite `dags/weather_pipeline_dag.py`:
```python
schedule_interval='0 */3 * * *',  # A cada 3 horas
```

---

## Recursos Adicionais

- **Documentação Completa**: Veja [README.md](README.md)
- **Exemplos de Código**: Veja pasta `examples/`
- **Testes**: Veja pasta `tests/`
- **Open-Meteo API**: https://open-meteo.com/en/docs

---

## Precisa de Ajuda?

1. Verifique os logs: `data/cron.log`
2. Execute com modo verbose: `pytest tests/ -vv`
3. Abra uma issue no GitHub
4. Consulte a documentação da API

---

**Boa análise! 🌤️📊**