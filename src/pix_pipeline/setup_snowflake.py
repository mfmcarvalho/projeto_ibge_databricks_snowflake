"""Módulo `setup_snowflake` — ponte de importação do notebook.

Notebooks Databricks (arquivos .ipynb) não podem ser importados
diretamente via sys.path. Este arquivo torna o notebook
`setup_snowflake.ipynb` (nesta pasta) importável como módulo Python,
mantendo o notebook como fonte única do código.

Ao importar, executa apenas a CÉLULA 0 do notebook, que contém as
definições (`log`, `executar_setup_snowflake`, ...). Se reorganizar
o notebook, mantenha as definições na primeira célula.
"""

import json as _json
import os as _os

_CAMINHO_NOTEBOOK = _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)),
    "setup_snowflake.ipynb",
)

with open(_CAMINHO_NOTEBOOK, encoding="utf-8") as _arquivo:
    _codigo = "".join(
        _json.load(_arquivo)["cells"][0]["source"]
    )

exec(
    compile(_codigo, _CAMINHO_NOTEBOOK, "exec"),
    globals(),
)
