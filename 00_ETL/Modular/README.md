# Split inicial por import do notebook 01_00_ENTIDADES_E_RELACIONAMENTOS(1).ipynb

Estrutura gerada automaticamente a partir do notebook enviado.

## Arquivos
- `contexto.py`: imports, path e utilitários base
- `etapa_00_orquestrador.py`: execução central por import
- `etapa_01_ent_pessoa.py`
- `etapa_02_localizacao_cela.py`
- `etapa_03_prontuarios_alvaras_encarceramento.py`
- `etapa_04_fat_visita_advogado.py`
- `etapa_05_fat_visita_familiar.py`
- `etapa_06_fat_visita_religiosa.py`
- `etapa_07_fat_ocorrencia_infopen.py`
- `etapa_08_fat_ocorrencia_livro.py`
- `etapa_09_ent_movimentacao.py`
- `debug_snippets.py`

## Exemplo de uso
```python
from etapa_00_orquestrador import executar_todas
executar_todas(spark, path)
```

## Observação
Este split preserva o código do notebook praticamente como estava, apenas agrupado por etapa e encapsulado em funções `executar(spark, path)`.
