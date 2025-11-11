# 📤 Instruções para Entrega - RA 6324073

## ✅ Estrutura Criada

A pasta `6324073/` foi criada com todos os arquivos obrigatórios para entrega:

```
6324073/
├── pipeline_produtos_vendas.py    # DAG completa do pipeline ETL
├── README.md                       # Documentação da solução
└── dados/                          # Arquivos CSV utilizados
    ├── produtos_loja.csv
    └── vendas_produtos.csv
```

---

## 🔄 Passos para Entrega no GitHub

### 1. Fazer Fork do Repositório

1. Acessar: https://github.com/leonardofsano/data-pipeline-workshop-airflow3
2. Clicar em **"Fork"** no canto superior direito
3. Aguardar criação do fork na sua conta

### 2. Clonar o Fork (se ainda não fez)

```bash
git clone https://github.com/[SEU_USUARIO]/data-pipeline-workshop-airflow3.git
cd data-pipeline-workshop-airflow3
```

### 3. Adicionar os Arquivos da Pasta 6324073

A pasta já está criada localmente. Você precisa fazer commit e push:

```powershell
# Adicionar todos os arquivos da pasta 6324073
git add 6324073/

# Fazer commit com mensagem descritiva
git commit -m "[6324073] - [SEU NOME COMPLETO] - Exercício Final"

# Push para o seu fork
git push origin main
```

### 4. Criar Pull Request

1. Acessar seu fork no GitHub
2. Clicar em **"Pull requests"**
3. Clicar em **"New pull request"**
4. Verificar que está comparando:
   - **Base repository:** `leonardofsano/data-pipeline-workshop-airflow3` (base: main)
   - **Head repository:** `[seu-usuario]/data-pipeline-workshop-airflow3` (compare: main)
5. Clicar em **"Create pull request"**

### 5. Preencher Informações do PR

**Título:**
```
[6324073] - [SEU NOME COMPLETO] - Exercício Final
```

**Exemplo:**
```
[6324073] - João Silva Santos - Exercício Final
```

**Descrição:**
```markdown
## Resumo da Implementação

Pipeline ETL completo para processamento de dados de produtos e vendas.

### Implementação
- ✅ 6 tasks principais + 1 bônus
- ✅ Tratamento de dados nulos conforme especificação
- ✅ Cálculos de receita e margem de lucro
- ✅ Relatórios analíticos completos
- ✅ Detecção de produtos com baixa performance (BÔNUS)

### Tecnologias
- Apache Airflow
- Python + Pandas
- PostgreSQL
- Docker

### Abordagem
ETL (Extract, Transform, Load) - justificado pela natureza dos dados e requisitos do projeto.

### Arquivos Entregues
- `pipeline_produtos_vendas.py` - DAG completa
- `README.md` - Documentação detalhada
- `dados/` - Arquivos CSV utilizados
```

6. Clicar em **"Create pull request"**

---

## 📋 Checklist Final

Antes de submeter, verifique:

- [ ] Pasta `6324073/` criada na raiz do projeto
- [ ] Arquivo `pipeline_produtos_vendas.py` presente
- [ ] Arquivo `README.md` com documentação completa
- [ ] Pasta `dados/` com os 2 arquivos CSV
- [ ] Seu nome está no README.md
- [ ] Código está comentado e legível
- [ ] Pull Request criado com título correto
- [ ] Descrição do PR está completa

---

## 🎯 Estrutura Esperada no Repositório

Após o merge, a estrutura será:

```
data-pipeline-workshop-airflow3/
├── [outros_alunos]/
├── 6324073/                        # ← Sua entrega
│   ├── pipeline_produtos_vendas.py
│   ├── README.md
│   └── dados/
│       ├── produtos_loja.csv
│       └── vendas_produtos.csv
├── dags/
├── data/
├── db/
├── docker-compose.yml
└── EXERCICIO_FINAL.md
```

---

## ⚠️ Atenção

1. **Não modifique arquivos fora da pasta `6324073/`**
2. **Use exatamente o número do seu RA como nome da pasta**
3. **Preencha seu nome completo no README.md** (linha 3)
4. **Título do PR deve seguir o formato especificado**
5. **Teste localmente antes de fazer o PR**

---

## 🧪 Teste Local (Opcional mas Recomendado)

Antes de fazer o PR, teste o pipeline:

```bash
# Copiar DAG para a pasta dags
cp 6324073/pipeline_produtos_vendas.py dags/

# Iniciar Airflow
docker-compose up -d

# Acessar UI e executar
# http://localhost:5000

# Verificar se todas as tasks passam
```

Se tudo funcionar, pode submeter o PR com confiança! ✅

---

## 📞 Dúvidas?

- Revisar `EXERCICIO_FINAL.md` para requisitos
- Consultar `README.md` na pasta 6324073 para detalhes da implementação
- Verificar `TESTE_PIPELINE.md` na raiz para guia de execução

---

**Boa sorte! 🚀**

*Lembre-se: O importante é demonstrar compreensão dos conceitos de ETL e boas práticas de engenharia de dados.*
