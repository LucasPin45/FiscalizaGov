# scripts/atualizar_gastos.py - gera data/gastos_AAAA.csv (rodado de madrugada pelo GitHub Actions)
# Uso: TRANSPARENCIA_API_KEY=... python scripts/atualizar_gastos.py [ano ...]
# So grava o arquivo de um ano se a coleta daquele ano foi COMPLETA; senao mantem o anterior e sai com 1.
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import dou  # noqa: E402
import gastos  # noqa: E402


def main(argv) -> int:
    chave = os.getenv("TRANSPARENCIA_API_KEY", "")
    if not chave:
        print("Defina TRANSPARENCIA_API_KEY.", file=sys.stderr)
        return 1
    ano_atual = dou.hoje_brasilia().year
    anos = [int(a) for a in argv] or [ano_atual, ano_atual - 1]
    agora = dou.agora_brasilia().strftime("%d/%m/%Y %H:%M")
    falhou = False
    for ano in anos:
        df, erros = gastos.coletar_ano(ano, chave)
        if erros or df.empty:
            print(f"{ano}: coleta incompleta, arquivo anterior mantido. Erros: {erros[:5]}", file=sys.stderr)
            falhou = True
            continue
        gastos.salvar_snapshot(df, ano, agora=agora)
        print(f"{ano}: {len(df)} órgãos gravados ({agora})")
    return 1 if falhou else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
