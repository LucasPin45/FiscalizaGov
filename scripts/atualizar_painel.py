# scripts/atualizar_painel.py - gera data/painel.json (arrecadacao e gasto do ano, para o relogio da Apresentacao)
# Baixa os arquivos da CGU (receitas ~1,7 MB e despesas ~0,8 MB), soma e grava um JSON de poucos KB.
# Se algo falhar, mantem o arquivo anterior e sai com codigo 1 (falha nunca vira zero).
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import dou  # noqa: E402
import painel  # noqa: E402
from dou import ErroColeta  # noqa: E402


def main() -> int:
    ano = dou.hoje_brasilia().year
    try:
        p = painel.calcular_ao_vivo(ano)
    except (ErroColeta, ValueError) as e:
        print(f"Falhou, arquivo anterior mantido: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    p["gerado_em"] = dou.agora_brasilia().strftime("%d/%m/%Y %H:%M")
    painel.salvar(p)
    r, g = p["receita"], p["gasto"]
    print(f"{ano}: arrecadação R$ {r['arrecadacao']/1e9:,.1f} bi (até {r['asof']}), gasto R$ {g['gasto_liquidado']/1e9:,.1f} bi "
          f"(arquivo de {g['asof']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
