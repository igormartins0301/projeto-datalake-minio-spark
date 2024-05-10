# Objetivo do Projeto
Desenvolver um ambiente de Data Lake local separado em 3 zonas:
- Landing: recebe os dados de origem.
- Curated: recebe os dados de origem convertidos em parquet para otimizar as transformações.
- Processing: recebe em parquet uma versão agregada dos dados com regra de negócio.

### Ferramentas utilizadas
- MinIO: Organização do Data Lake local
- Spark: Motor de processamento dos dados entre as camadas

### Link da base utilizada
https://www.kaggle.com/datasets/nhs/general-practice-prescribing-data


# Desenho da arquitetura
![image](/img/Arch.png)